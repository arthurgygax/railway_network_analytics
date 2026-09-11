"""Offline tests for the DB Timetables adapter. No network, no wall clock."""

from __future__ import annotations

import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from railway_network_analytics.db_timetables import (
    BERLIN,
    DbTimetablesSource,
    PlanCache,
    TimetablesClient,
    TimetablesUnavailable,
    fernverkehr_only,
    parse_change_stop,
    parse_plan_stop,
    split_stop_id,
)

FIXED_NOW = datetime(2026, 9, 10, 12, 0, tzinfo=ZoneInfo("Europe/Berlin"))
STOP_ID = "3704713885565394549-2609101024-9"

PLAN_XML = f"""<timetable station='Frankfurt(Main)Hbf'>
 <s id="{STOP_ID}">
   <tl f="F" t="p" o="80" c="ICE" n="29"/>
   <ar pt="2609101359" pp="Fern 5" ppth="Köln Hbf|Siegburg/Bonn"/>
   <dp pt="2609101402" pp="Fern 5" ppth="Frankfurt(Main)Hbf|Hanau Hbf|Würzburg Hbf"/>
 </s>
 <s id="9999-2609101100-1">
   <tl f="N" t="p" o="800772" c="RE" n="4608"/>
   <dp pt="2609101115" pp="4"/>
 </s>
</timetable>"""

FCHG_XML = f"""<timetable station='Frankfurt(Main)Hbf' eva="8070003">
 <s id="{STOP_ID}" eva="8070003">
   <ar ct="2609101407" cp="9"/>
   <dp ct="2609101409"/>
   <m id="m1" t="d" c="43"/>
   <m id="m2" t="h" cat="Störung"/>
 </s>
 <s id="9999-2609101100-1" eva="8070003"><dp ct="2609101120"/></s>
 <s id="-777-2609100500-3" eva="8070003"><ar ct="2609101500"/></s>
</timetable>"""

EMPTY = "<timetable/>"
TARGET = {"eva": 8070003, "name": "Frankfurt(M) Flughafen Fernbf"}


class StubClient:
    """Answers scripted paths; anything else is an empty timetable."""

    def __init__(self, responses: dict[str, str] | None = None) -> None:
        self.responses = responses or {}
        self.paths: list[str] = []
        self.requests = 0

    def get(self, path: str) -> ET.Element:
        self.paths.append(path)
        self.requests += 1
        body = self.responses.get(path)
        if body is None:
            body = self.responses.get(path.split("/")[0], EMPTY)
        if isinstance(body, Exception):
            raise body
        return ET.fromstring(body)


def make_source(tmp_path: Path, responses=None, targets=None) -> DbTimetablesSource:
    return DbTimetablesSource(
        client=StubClient(
            responses if responses is not None else {"plan": PLAN_XML, "fchg": FCHG_XML}
        ),
        targets=targets if targets is not None else [TARGET],
        cache=PlanCache(tmp_path / "plan_cache.json"),
        clock=lambda: FIXED_NOW,
    )


# --------------------------------------------------------------------- parsing


def test_split_stop_id():
    assert split_stop_id(STOP_ID) == ("3704713885565394549", "2609101024", 9)


def test_split_stop_id_handles_negative_trip_ids():
    """DB trip ids are signed 64-bit and frequently negative — rsplit, never split."""
    assert split_stop_id("-4676367904559688889-2609100845-5") == (
        "-4676367904559688889", "2609100845", 5,
    )


def test_parse_plan_stop_extracts_train_identity():
    """<tl> is the field GTFS.de never had: it names the actual train."""
    stop = ET.fromstring(PLAN_XML).find("s")
    parsed = parse_plan_stop(stop)
    assert parsed["train_category"] == "ICE"
    assert parsed["train_number"] == "29"
    assert parsed["train_operator"] == "80"
    assert parsed["train_filter"] == "F"
    assert parsed["planned_arrival"] == "2609101359"
    assert parsed["planned_departure"] == "2609101402"
    assert parsed["planned_arrival_platform"] == "Fern 5"
    assert parsed["planned_path_from"].startswith("Köln Hbf|")     # where it came from
    assert parsed["planned_path_to"].startswith("Frankfurt(Main)Hbf|")  # where it goes


def test_parse_plan_stop_tolerates_missing_arrival():
    """An origin station has a departure but no arrival."""
    parsed = parse_plan_stop(ET.fromstring(PLAN_XML).findall("s")[1])
    assert parsed["planned_arrival"] is None
    assert parsed["planned_departure"] == "2609101115"


def test_parse_change_stop_extracts_changes_and_messages():
    stop = ET.fromstring(FCHG_XML).find("s")
    parsed = parse_change_stop(stop)
    assert parsed["changed_arrival"] == "2609101407"
    assert parsed["changed_departure"] == "2609101409"
    assert parsed["changed_arrival_platform"] == "9"
    assert ("d", "43", None) in parsed["messages"]
    assert ("h", None, "Störung") in parsed["messages"]


def test_parse_change_stop_with_no_changes_yields_nones():
    parsed = parse_change_stop(ET.fromstring("<s id='x'/>"))
    assert parsed["changed_arrival"] is None
    assert parsed["messages"] == ()


# ----------------------------------------------------------------------- join


def test_changes_are_joined_to_the_plan(tmp_path):
    result = make_source(tmp_path).poll()
    observed = {o.stop_id: o for o in result.observations}
    ice = observed[STOP_ID]
    assert (ice.train_category, ice.train_number) == ("ICE", "29")
    assert (ice.planned_arrival, ice.changed_arrival) == ("2609101359", "2609101407")
    assert ice.changed_arrival_platform == "9"
    assert ice.station_eva == 8070003


def test_unmatched_changes_are_kept_not_dropped(tmp_path):
    """A change whose plan hour fell outside the window is often a badly delayed
    train. Dropping what we cannot identify would lose the most interesting records."""
    result = make_source(tmp_path).poll()
    orphan = next(o for o in result.observations if o.stop_id == "-777-2609100500-3")
    assert orphan.train_category is None      # unidentified
    assert orphan.changed_arrival == "2609101500"  # but the change is preserved
    assert result.unidentified == 1
    assert result.identified == 2  # counted before scope filtering


def test_counters_report_the_join_quality(tmp_path):
    counters = make_source(tmp_path).poll().counters()
    assert counters["stations_polled"] == 1
    assert counters["stations_failed"] == 0
    assert counters["changed_stops"] == 3
    # The RE is positively identified as Nahverkehr and excluded; the ICE and the
    # unidentified stop both survive.
    assert counters["observations"] == 2


def test_fernverkehr_filter_replaces_the_static_gtfs_join(tmp_path):
    """f="F" is the whole Fernverkehr filter now — no static feed, no trip-id set."""
    result = make_source(tmp_path).poll()
    fern = list(fernverkehr_only(result.observations))
    assert [o.train_number for o in fern] == ["29"]


def test_observed_at_comes_from_the_clock(tmp_path):
    (obs, *_) = make_source(tmp_path).poll().observations
    assert obs.observed_at == FIXED_NOW.isoformat()


def test_payload_excludes_observed_at(tmp_path):
    """Change detection must ignore our own timestamp or it suppresses nothing."""
    a = make_source(tmp_path).poll().observations[0]
    later = DbTimetablesSource(
        client=StubClient({"plan": PLAN_XML, "fchg": FCHG_XML}),
        targets=[TARGET],
        cache=PlanCache(tmp_path / "c2.json"),
        clock=lambda: datetime(2026, 9, 10, 18, 0, tzinfo=BERLIN),
    ).poll().observations[0]
    assert a.observed_at != later.observed_at
    assert a.payload() == later.payload()
    assert a.key() == later.key()


# --------------------------------------------------------------- plan caching


def test_cold_cache_fetches_the_whole_window(tmp_path):
    source = make_source(tmp_path)
    source.poll()
    plan_calls = [p for p in source.client.paths if p.startswith("plan/")]
    assert len(plan_calls) == 18  # -9h .. +8h


def test_warm_cache_refetches_only_the_current_and_next_hour(tmp_path):
    """Past plan hours are immutable; refetching them every cycle is pure waste."""
    cache_path = tmp_path / "plan_cache.json"
    first = make_source(tmp_path)
    first.poll()

    second = DbTimetablesSource(
        client=StubClient({"plan": PLAN_XML, "fchg": FCHG_XML}),
        targets=[TARGET],
        cache=PlanCache(cache_path),
        clock=lambda: FIXED_NOW,
    )
    result = second.poll()
    assert len([p for p in second.client.paths if p.startswith("plan/")]) == 2
    assert result.plan_cache_hits == 16
    assert result.observations  # still fully joined from cache


def test_cache_survives_a_restart(tmp_path):
    cache_path = tmp_path / "plan_cache.json"
    make_source(tmp_path).poll()
    assert cache_path.is_file()
    assert PlanCache(cache_path).stops[STOP_ID]["train_number"] == "29"


def test_cache_prunes_entries_older_than_keep_days(tmp_path):
    cache = PlanCache(tmp_path / "c.json", keep_days=2)
    cache.stops = {
        "1-2609100800-1": {},   # 2026-09-10, keep
        "2-2609070800-1": {},   # 2026-09-07, stale
    }
    assert cache.prune(FIXED_NOW) == 1
    assert list(cache.stops) == ["1-2609100800-1"]


# -------------------------------------------------------------- failure modes


def test_a_failing_station_does_not_abort_the_cycle(tmp_path):
    """One station returning 503 must not lose the other 59."""
    client = StubClient({"plan": PLAN_XML, "fchg": FCHG_XML})
    good = dict(TARGET)
    bad = {"eva": 999, "name": "Broken"}

    real_get = client.get

    def get(path: str):
        if path == "fchg/999":
            raise TimetablesUnavailable("HTTP 503")
        return real_get(path)

    client.get = get
    source = DbTimetablesSource(
        client=client, targets=[bad, good],
        cache=PlanCache(tmp_path / "c.json"), clock=lambda: FIXED_NOW,
    )
    result = source.poll()
    assert result.stations_failed == 1
    assert result.stations_polled == 1
    assert result.observations


def test_http_error_becomes_timetables_unavailable(monkeypatch):
    def boom(request, timeout):  # noqa: ARG001
        raise urllib.error.HTTPError("u", 503, "boom", {}, None)

    monkeypatch.setattr(urllib.request, "urlopen", boom)
    monkeypatch.setattr("railway_network_analytics.db_timetables.RATE_LIMIT_SLEEP", 0)
    with pytest.raises(TimetablesUnavailable, match="HTTP 503"):
        TimetablesClient("id", "key").get("fchg/1")


def test_unparseable_xml_becomes_timetables_unavailable(monkeypatch):
    class Response:
        def read(self):
            return b"<html>rate limited</html>"

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

    monkeypatch.setattr(urllib.request, "urlopen", lambda request, timeout: Response())
    monkeypatch.setattr("railway_network_analytics.db_timetables.RATE_LIMIT_SLEEP", 0)
    with pytest.raises(TimetablesUnavailable, match="unparseable"):
        TimetablesClient("id", "key").get("fchg/1")


def test_stop_without_an_id_is_skipped(tmp_path):
    source = make_source(tmp_path, responses={
        "plan": EMPTY, "fchg": "<timetable><s/><s id='1-2609101000-2'/></timetable>",
    })
    result = source.poll()
    assert result.changed_stops == 2
    assert len(result.observations) == 1


def test_known_nahverkehr_is_excluded_but_unidentified_is_kept(tmp_path):
    """Scope filtering is asymmetric on purpose: exclude what we can prove is regional,
    keep what we simply could not identify. Unidentified stops skew towards trains that
    started long ago, i.e. the badly delayed ones."""
    result = make_source(tmp_path).poll()
    filters = sorted((o.train_filter or "unknown") for o in result.observations)
    assert filters == ["F", "unknown"]


def test_scope_filter_can_be_disabled(tmp_path):
    source = make_source(tmp_path)
    source.only_fernverkehr = False
    assert len(source.poll().observations) == 3


def test_third_party_operators_have_no_filter_flag_and_are_kept(tmp_path):
    """Measured on live data: DB sets `f` only for its own and partner trains, so a
    non-DB operator's <tl> carries a category but no flag. That bucket mixes private
    regional (NX) with open-access long-distance (FLX), so we keep it and let Silver
    classify — a category allowlist here would silently drop Flixtrain."""
    plan = """<timetable>
     <s id="1-2609101000-1"><tl t="p" o="FLX30" c="FLX" n="1322"/><dp pt="2609101010"/></s>
     <s id="2-2609101000-1"><tl t="p" o="NXRE" c="NX" n="89723"/><dp pt="2609101020"/></s>
     <s id="3-2609101000-1"><tl f="N" t="p" o="80" c="RE" n="1"/><dp pt="2609101030"/></s>
    </timetable>"""
    fchg = """<timetable>
     <s id="1-2609101000-1"><dp ct="2609101015"/></s>
     <s id="2-2609101000-1"><dp ct="2609101025"/></s>
     <s id="3-2609101000-1"><dp ct="2609101035"/></s>
    </timetable>"""
    result = make_source(tmp_path, responses={"plan": plan, "fchg": fchg}).poll()
    kept = {o.train_category for o in result.observations}
    assert kept == {"FLX", "NX"}   # both unflagged: kept, noise and signal alike
    assert "RE" not in kept        # positively flagged Nahverkehr: excluded


def test_cancellation_status_is_captured():
    """cs="c" is the ONLY cancellation signal this feed carries. Dropping it makes
    cancellation rate — a stated dashboard metric — impossible to compute at all."""
    stop = ET.fromstring('<s id="1-2609101000-1">'
                         '<ar cs="c" clt="2609101740"/><dp cs="c" clt="2609101740"/></s>')
    parsed = parse_change_stop(stop)
    assert parsed["arrival_status"] == "c"
    assert parsed["departure_status"] == "c"
    assert parsed["cancelled_at"] == "2609101740"


def test_a_cancellation_appearing_counts_as_a_change(tmp_path):
    """The most important change possible must not be suppressed by change detection."""
    from railway_network_analytics.change_filter import digest

    running = make_source(tmp_path).poll().observations[0]
    cancelled = type(running)(**{**running.to_dict(), "arrival_status": "c"})
    assert digest(running.payload()) != digest(cancelled.payload())


def test_brand_recovers_identity_when_the_plan_is_missing(tmp_path):
    """fb appears in fchg where <tl> does not, so it names some of the ~14% of stops
    that never match a plan hour."""
    fchg = ('<timetable><s id="-9-2609100500-3" eva="1">'
            '<ar ct="2609101500" fb="ICE 2829"/></s></timetable>')
    (obs,) = make_source(tmp_path, responses={"plan": EMPTY, "fchg": fchg}).poll().observations
    assert obs.train_category is None   # no plan match
    assert obs.brand == "ICE 2829"      # but we still know which train it was


def test_arrival_and_departure_paths_are_kept_separate():
    """ar.ppth is where the train came FROM; dp.ppth is where it is going TO. One
    column for both would give it opposite meanings at different stops, corrupting any
    origin->destination route model."""
    stop = ET.fromstring(
        '<s id="1-2609101000-1">'
        '<ar ppth="Zürich HB|Basel SBB" pp="7"/>'
        '<dp ppth="Freiburg|Karlsruhe|Mannheim" pp="8"/></s>')
    parsed = parse_plan_stop(stop)
    assert parsed["planned_path_from"] == "Zürich HB|Basel SBB"
    assert parsed["planned_path_to"] == "Freiburg|Karlsruhe|Mannheim"
    assert parsed["planned_arrival_platform"] == "7"
    assert parsed["planned_departure_platform"] == "8"


def test_plan_cache_is_saved_per_station_not_once_per_cycle(tmp_path):
    """A cold cycle takes ~20 minutes of rate-limited requests. Saving only at the end
    means a restart in that window discards all of it — which is exactly what happened
    during setup, leaving the cache file empty after three rebuilds."""
    cache_path = tmp_path / "plan_cache.json"
    targets = [{"eva": 1, "name": "A"}, {"eva": 2, "name": "B"}]
    source = make_source(tmp_path, targets=targets)
    source.cache.path = cache_path

    saves = []
    original = source.cache.save
    source.cache.save = lambda: (saves.append(len(source.cache.stops)), original())
    source.poll()
    assert len(saves) >= len(targets)   # at least one save per station, not just one
