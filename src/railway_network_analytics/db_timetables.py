"""Deutsche Bahn Timetables API adapter.

Shape of the source, which drives every decision here:

  `plan/{eva}/{date}/{hour}`  the scheduled skeleton for one station-hour. Carries
                              train identity (<tl>: ICE 575), planned times, planned
                              platform and the full route path. Immutable once past.
  `fchg/{eva}`                every currently-known change for that station, over a
                              rolling ~28h window. A SPARSE delta: only 32 of 778
                              stops carry <tl>, so it cannot identify a train alone.

They share the stop `id`, which decomposes as {trip_id}-{start_datetime}-{stop_index}.
So: join fchg onto plan to get identity.

Because past plan hours never change, they are cached on disk. Without the cache a
cycle costs 18 plan requests per station; with it, roughly one.

Free tier is 60 requests/minute — the binding constraint on everything.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)

API = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1"
BERLIN = ZoneInfo("Europe/Berlin")

# Measured: -9h captures trains that departed long ago, +8h captures the evening.
# Together they identify ~90% of changes. Beyond that, returns flatten.
PLAN_HOURS_BACK = 9
PLAN_HOURS_FORWARD = 8

RATE_LIMIT_SLEEP = 1.1  # 60 req/min -> stay just under one per second


class TimetablesUnavailable(RuntimeError):
    """Upstream unreachable, rate-limited, or returned something unparseable."""


# ----------------------------------------------------------------------- model


@dataclass(frozen=True, slots=True)
class StopObservation:
    """One observation of one train calling at one station.

    The source is station-centric, so this — not the trip — is its natural unit.

    Planned vs changed is kept as the source gives it. Delay is `changed - planned`,
    a derived value that belongs downstream, not in ingestion.
    """

    stop_id: str
    station_eva: int
    station_name: str
    trip_id: str
    start_datetime: str
    stop_index: int
    train_category: str | None
    train_number: str | None
    train_operator: str | None
    train_filter: str | None
    planned_arrival: str | None
    changed_arrival: str | None
    planned_departure: str | None
    changed_departure: str | None
    planned_platform: str | None
    changed_platform: str | None
    planned_path: str | None
    changed_path: str | None
    messages: tuple[tuple[str | None, str | None, str | None], ...]
    observed_at: str

    def key(self) -> str:
        return self.stop_id

    def payload(self) -> tuple:
        """Everything except observed_at — what must differ for this to be new data."""
        return (
            self.changed_arrival, self.changed_departure, self.changed_platform,
            self.changed_path, self.messages,
        )

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PollResult:
    observations: list[StopObservation]
    stations_polled: int
    stations_failed: int
    changed_stops: int
    identified: int
    unidentified: int
    plan_requests: int
    plan_cache_hits: int

    def counters(self) -> dict[str, int]:
        d = asdict(self)
        d.pop("observations")
        d["observations"] = len(self.observations)
        return d


# --------------------------------------------------------------------- fetching


@dataclass
class TimetablesClient:
    """Thin HTTP wrapper. Sleeps between calls to respect the 60 req/min limit."""

    client_id: str
    api_key: str
    requests: int = field(default=0, init=False)

    def get(self, path: str) -> ET.Element:
        request = urllib.request.Request(
            f"{API}/{path}",
            headers={"DB-Client-Id": self.client_id, "DB-Api-Key": self.api_key},
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = response.read()
        except urllib.error.HTTPError as exc:
            raise TimetablesUnavailable(f"HTTP {exc.code} for {path}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise TimetablesUnavailable(f"cannot reach {path}: {exc}") from exc
        finally:
            self.requests += 1
            time.sleep(RATE_LIMIT_SLEEP)
        try:
            root = ET.fromstring(payload)
        except ET.ParseError as exc:
            raise TimetablesUnavailable(f"unparseable XML for {path}: {exc}") from exc
        # An HTML error page is often well-formed XML, so parsing alone proves nothing:
        # it would yield a root with no <s> children and read as "no changes".
        if root.tag != "timetable":
            raise TimetablesUnavailable(
                f"unparseable response for {path}: expected <timetable>, got <{root.tag}>"
            )
        return root


# ---------------------------------------------------------------------- parsing


def _attr(element: ET.Element | None, name: str) -> str | None:
    return element.get(name) if element is not None else None


def parse_plan_stop(stop: ET.Element) -> dict:
    """Identity + planned values. Everything here is immutable once the hour is past."""
    tl, ar, dp = stop.find("tl"), stop.find("ar"), stop.find("dp")
    return {
        "train_category": _attr(tl, "c"),
        "train_number": _attr(tl, "n"),
        "train_operator": _attr(tl, "o"),
        "train_filter": _attr(tl, "f"),
        "planned_arrival": _attr(ar, "pt"),
        "planned_departure": _attr(dp, "pt"),
        "planned_platform": _attr(ar, "pp") or _attr(dp, "pp"),
        "planned_path": _attr(dp, "ppth") or _attr(ar, "ppth"),
    }


def parse_change_stop(stop: ET.Element) -> dict:
    ar, dp = stop.find("ar"), stop.find("dp")
    return {
        "changed_arrival": _attr(ar, "ct"),
        "changed_departure": _attr(dp, "ct"),
        "changed_platform": _attr(ar, "cp") or _attr(dp, "cp"),
        "changed_path": _attr(dp, "cpth") or _attr(ar, "cpth"),
        "messages": tuple(
            (m.get("t"), m.get("c"), m.get("cat")) for m in stop.iter("m")
        ),
    }


def split_stop_id(stop_id: str) -> tuple[str, str, int]:
    """{trip_id}-{start_datetime}-{stop_index}. trip_id may itself be negative."""
    trip_id, start, index = stop_id.rsplit("-", 2)
    return trip_id, start, int(index)


# ------------------------------------------------------------------ plan cache


class PlanCache:
    """Disk cache of planned stops, keyed by stop id.

    Past plan hours are immutable, so re-fetching them every cycle is pure waste:
    18 requests per station per cycle becomes roughly one. Entries are pruned by the
    train's start date so the file cannot grow without bound.
    """

    def __init__(self, path: Path, keep_days: int = 2) -> None:
        self.path = path
        self.keep_days = keep_days
        self.stops: dict[str, dict] = {}
        self.hours_fetched: set[str] = set()
        if path.is_file():
            data = json.loads(path.read_text(encoding="utf-8"))
            self.stops = data.get("stops", {})
            self.hours_fetched = set(data.get("hours_fetched", []))

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps({"stops": self.stops, "hours_fetched": sorted(self.hours_fetched)}),
            encoding="utf-8",
        )

    def prune(self, now: datetime) -> int:
        cutoff = (now - timedelta(days=self.keep_days)).strftime("%y%m%d")
        stale = [k for k in self.stops if split_stop_id(k)[1][:6] < cutoff]
        for key in stale:
            del self.stops[key]
        self.hours_fetched = {h for h in self.hours_fetched if h.split("/")[1][:6] >= cutoff}
        return len(stale)

    def fill(self, client: TimetablesClient, eva: int, now: datetime) -> tuple[int, int]:
        """Fetch any plan hours in the window we have not already cached."""
        fetched = hits = 0
        for offset in range(-PLAN_HOURS_BACK, PLAN_HOURS_FORWARD + 1):
            when = now + timedelta(hours=offset)
            slot = f"{eva}/{when:%y%m%d%H}"
            # Past hours are immutable. The current and next hour can still gain
            # entries, so always refetch those; hours further out are refetched
            # naturally as they approach, at least twice before they arrive.
            if slot in self.hours_fetched and offset not in (0, 1):
                hits += 1
                continue
            try:
                root = client.get(f"plan/{eva}/{when:%y%m%d}/{when:%H}")
            except TimetablesUnavailable as exc:
                log.warning("plan fetch failed", extra={"eva": eva, "detail": str(exc)})
                continue
            for stop in root.findall("s"):
                self.stops[stop.get("id")] = parse_plan_stop(stop)
            self.hours_fetched.add(slot)
            fetched += 1
        return fetched, hits


# --------------------------------------------------------------------- adapter


@dataclass
class DbTimetablesSource:
    """Poll every target station's changes and join them onto the cached plan."""

    client: TimetablesClient
    targets: list[dict]
    cache: PlanCache
    clock: object = None  # callable returning an aware datetime; defaults to now()
    # Project scope. Excludes what is positively flagged as NOT long-distance
    # (f=N Nahverkehr, f=S S-Bahn, f=D partner), and keeps everything else.
    #
    # That asymmetry matters more than it looks, because `f` is missing in two very
    # different situations and we cannot tell them apart here:
    #
    #   1. No plan match at all — the stop's plan hour fell outside our window. These
    #      skew towards trains that started long ago, i.e. the badly delayed ones.
    #   2. A non-DB operator. DB sets `f` only for its own and partner trains, so
    #      third-party operators have no flag — and that bucket mixes private REGIONAL
    #      (NX National Express, ARV, vlx) with open-access LONG-DISTANCE (FLX
    #      Flixtrain, TRI). Measured on live data, both appear.
    #
    # So `f="F"` is a DB-centric filter, not a mode classifier. A category allowlist
    # would be a classifier, but it would drop Flixtrain and rot as operators change.
    # We over-collect deliberately and let Silver classify on category + operator.
    only_fernverkehr: bool = True

    def _now(self) -> datetime:
        return self.clock() if self.clock else datetime.now(BERLIN)

    def poll(self) -> PollResult:
        now = self._now()
        observed_at = now.isoformat()
        removed = self.cache.prune(now)
        if removed:
            log.info("pruned stale plan entries", extra={"removed": removed})

        observations: list[StopObservation] = []
        polled = failed = changed = identified = unidentified = 0
        plan_requests = plan_hits = 0

        for target in self.targets:
            eva = target["eva"]
            fetched, hits = self.cache.fill(self.client, eva, now)
            plan_requests += fetched
            plan_hits += hits
            try:
                root = self.client.get(f"fchg/{eva}")
            except TimetablesUnavailable as exc:
                failed += 1
                log.warning("fchg fetch failed", extra={"eva": eva, "detail": str(exc)})
                continue
            polled += 1
            for stop in root.findall("s"):
                changed += 1
                observation = self._build(stop, target, observed_at)
                if observation is None:
                    continue
                if observation.train_category is None:
                    unidentified += 1
                else:
                    identified += 1
                if self.only_fernverkehr and observation.train_filter not in (None, "F"):
                    continue
                observations.append(observation)

        self.cache.save()
        return PollResult(
            observations=observations,
            stations_polled=polled,
            stations_failed=failed,
            changed_stops=changed,
            identified=identified,
            unidentified=unidentified,
            plan_requests=plan_requests,
            plan_cache_hits=plan_hits,
        )

    def _build(self, stop: ET.Element, target: dict, observed_at: str) -> StopObservation | None:
        stop_id = stop.get("id")
        if not stop_id:
            return None
        try:
            trip_id, start, index = split_stop_id(stop_id)
        except ValueError:
            log.warning("unparseable stop id", extra={"stop_id": stop_id})
            return None
        # Unidentified changes are kept, not dropped: a train whose plan hour fell
        # outside our window is often a badly delayed one, and those matter most.
        planned = self.cache.stops.get(stop_id, {})
        return StopObservation(
            stop_id=stop_id,
            station_eva=target["eva"],
            station_name=target["name"],
            trip_id=trip_id,
            start_datetime=start,
            stop_index=index,
            observed_at=observed_at,
            **{k: planned.get(k) for k in (
                "train_category", "train_number", "train_operator", "train_filter",
                "planned_arrival", "planned_departure", "planned_platform", "planned_path")},
            **parse_change_stop(stop),
        )


def fernverkehr_only(observations: list[StopObservation]) -> Iterator[StopObservation]:
    """`<tl f="F">` replaces the entire static-GTFS trip-id filter we used before."""
    return (o for o in observations if o.train_filter == "F")
