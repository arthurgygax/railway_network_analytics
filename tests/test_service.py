"""Loop behaviour: scheduling, failure policy, change detection, sink handoff.

Source-agnostic — the adapter itself is covered by test_db_timetables.py.
"""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from pathlib import Path

import pytest

from railway_network_analytics.change_filter import ChangeFilter
from railway_network_analytics.db_timetables import (
    PollResult,
    StopObservation,
    TimetablesUnavailable,
)
from railway_network_analytics.service import IngestionService
from railway_network_analytics.sink import JsonLinesSink


def observation(stop_id: str = "1-2609101000-3", **overrides) -> StopObservation:
    base = dict(
        stop_id=stop_id, station_eva=8000105, station_name="Frankfurt(Main)Hbf",
        trip_id="1", start_datetime="2609101000", stop_index=3,
        train_category="ICE", train_number="575", train_operator="80", train_filter="F",
        planned_arrival="2609101114", changed_arrival="2609101120",
        planned_departure="2609101120", changed_departure=None,
        planned_arrival_platform="7", planned_departure_platform="7",
        changed_arrival_platform=None, changed_departure_platform=None,
        planned_path_from="Köln Hbf|Siegburg/Bonn",
        planned_path_to="Mannheim Hbf|Stuttgart Hbf",
        changed_path_from=None, changed_path_to=None,
        arrival_status=None, departure_status=None, cancelled_at=None, brand="ICE 575",
        messages=(("d", "43", None),), observed_at="2026-09-10T12:00:00+02:00",
    )
    return StopObservation(**{**base, **overrides})


def poll_result(observations) -> PollResult:
    observations = list(observations)
    return PollResult(
        observations=observations, stations_polled=1, stations_failed=0,
        changed_stops=len(observations), identified=len(observations), unidentified=0,
        plan_requests=0, plan_cache_hits=0,
    )


class StubSource:
    """Replays scripted poll outcomes; an Exception instance is raised."""

    def __init__(self, *outcomes) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0

    def poll(self) -> PollResult:
        self.calls += 1
        outcome = self.outcomes[min(self.calls - 1, len(self.outcomes) - 1)]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class RecordingSink:
    def __init__(self) -> None:
        self.batches: list[list] = []

    def write(self, observations) -> int:
        batch = list(observations)
        self.batches.append(batch)
        return len(batch)

    @property
    def all_written(self) -> list:
        return [o for b in self.batches for o in b]


def build(source, sink, **kwargs) -> IngestionService:
    return IngestionService(
        source=source, sink=sink,
        poll_interval_seconds=kwargs.pop("poll_interval_seconds", 0), **kwargs,
    )


# ---------------------------------------------------------------------- loop


def test_single_poll_writes_observations():
    sink = RecordingSink()
    stats = build(StubSource(poll_result([observation()])), sink,
                  max_polls=1).run(threading.Event())
    assert stats.polls == 1
    assert stats.observations_written == 1
    assert sink.all_written[0].train_number == "575"


def test_api_failure_does_not_stop_the_loop():
    """A 503 must not kill a long-running ingestion. Because fchg carries a ~28h
    window, the next cycle recovers whatever this one missed."""
    sink = RecordingSink()
    service = build(
        StubSource(TimetablesUnavailable("HTTP 503"), poll_result([observation()])),
        sink, max_polls=2,
    )
    stats = service.run(threading.Event())
    assert stats.failures == 1
    assert stats.observations_written == 1


def test_empty_poll_is_tolerated():
    sink = RecordingSink()
    stats = build(StubSource(poll_result([])), sink, max_polls=1).run(threading.Event())
    assert stats.failures == 0
    assert stats.observations_written == 0
    assert sink.batches == []  # nothing to write means the sink is never called


def test_stop_event_ends_the_loop_before_the_next_poll():
    source = StubSource(poll_result([observation()]))
    service = build(source, RecordingSink(), poll_interval_seconds=3600)
    stop = threading.Event()
    stop.set()
    service.run(stop)
    assert source.calls == 0


def test_stop_event_set_during_the_wait_ends_the_loop():
    source = StubSource(poll_result([observation()]))
    service = build(source, RecordingSink(), poll_interval_seconds=3600)
    stop = threading.Event()
    threading.Timer(0.05, stop.set).start()
    service.run(stop)
    assert source.calls == 1
    assert service.stats.polls == 1


# --------------------------------------------------- change detection wiring


def test_unchanged_observations_are_suppressed(tmp_path):
    """fchg overlaps heavily between cycles; without this most output is repeats."""
    sink = RecordingSink()
    same = poll_result([observation()])
    stats = build(StubSource(same, same), sink, max_polls=2,
                  change_filter=ChangeFilter(tmp_path / "seen.json")).run(threading.Event())
    assert stats.observations_seen == 2
    assert stats.observations_written == 1
    assert stats.observations_suppressed == 1


def test_a_changed_delay_is_written_again(tmp_path):
    sink = RecordingSink()
    build(
        StubSource(poll_result([observation()]),
                   poll_result([observation(changed_arrival="2609101200")])),
        sink, max_polls=2, change_filter=ChangeFilter(tmp_path / "seen.json"),
    ).run(threading.Event())
    assert [o.changed_arrival for o in sink.all_written] == ["2609101120", "2609101200"]


def test_change_state_is_persisted_each_cycle(tmp_path):
    """An hourly run is cron-shaped: state must outlive the process."""
    state = tmp_path / "seen.json"
    build(StubSource(poll_result([observation()])), RecordingSink(), max_polls=1,
          change_filter=ChangeFilter(state)).run(threading.Event())
    assert state.is_file()
    assert json.loads(state.read_text())


def test_without_a_change_filter_everything_is_written():
    sink = RecordingSink()
    same = poll_result([observation()])
    stats = build(StubSource(same, same), sink, max_polls=2).run(threading.Event())
    assert stats.observations_written == 2
    assert stats.observations_suppressed == 0


# ---------------------------------------------------------------------- sink


def test_jsonl_sink_writes_one_valid_json_object_per_line(tmp_path):
    sink = JsonLinesSink(tmp_path)
    assert sink.write([observation()]) == 1
    lines = sink.path_for(datetime.now(UTC)).read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["train_category"] == "ICE"
    assert record["train_number"] == "575"
    assert record["changed_departure"] is None


def test_jsonl_sink_appends_across_calls(tmp_path):
    sink = JsonLinesSink(tmp_path)
    sink.write([observation()])
    sink.write([observation()])
    assert len(sink.path_for(datetime.now(UTC)).read_text().splitlines()) == 2


def test_jsonl_sink_writing_nothing_creates_no_file(tmp_path):
    sink = JsonLinesSink(tmp_path / "out")
    assert sink.write([]) == 0
    assert not (tmp_path / "out").exists()


def test_sink_write_probe_detects_unwritable_directory(tmp_path):
    """The failure we actually hit in Docker: a bind mount the container cannot write."""
    readonly = tmp_path / "ro"
    readonly.mkdir()
    readonly.chmod(0o500)
    try:
        with pytest.raises(OSError):
            JsonLinesSink(readonly / "out").ensure_writable()
    finally:
        readonly.chmod(0o700)


def test_sink_write_probe_leaves_no_residue(tmp_path):
    sink = JsonLinesSink(tmp_path / "out")
    sink.ensure_writable()
    assert list(Path(tmp_path / "out").iterdir()) == []


def test_kafka_client_logging_is_silenced_unless_debug():
    """kafka-python logs ~40 INFO lines per connection, burying the app's own output."""
    import logging

    from railway_network_analytics.logging_setup import configure_logging

    root = logging.getLogger()
    saved = (root.handlers[:], root.level, logging.getLogger("kafka").level)
    try:
        configure_logging("INFO", "text")
        assert logging.getLogger("kafka").level == logging.WARNING
        configure_logging("DEBUG", "text")
        assert logging.getLogger("kafka").level != logging.WARNING
    finally:
        root.handlers[:], root.level = saved[0], saved[1]
        logging.getLogger("kafka").setLevel(saved[2])
