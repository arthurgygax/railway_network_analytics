"""The ingestion loop.

Owns what the adapter does not: scheduling, failure policy, and deciding which
records are new enough to be worth emitting.

Source-agnostic: it needs a `poll()` returning something with `.observations` and
`.counters()`, and a sink taking a list of records.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import asdict, dataclass, field

from .change_filter import ChangeFilter
from .db_timetables import DbTimetablesSource, TimetablesUnavailable
from .sink import ObservationSink

log = logging.getLogger(__name__)


@dataclass
class ServiceStats:
    polls: int = 0
    failures: int = 0
    observations_seen: int = 0
    observations_written: int = 0
    observations_suppressed: int = 0

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


@dataclass
class IngestionService:
    source: DbTimetablesSource
    sink: ObservationSink
    poll_interval_seconds: int
    max_polls: int = 0  # 0 = run until stopped
    change_filter: ChangeFilter | None = None
    stats: ServiceStats = field(default_factory=ServiceStats)

    def run(self, stop: threading.Event) -> ServiceStats:
        log.info(
            "ingestion starting",
            extra={
                "poll_interval_seconds": self.poll_interval_seconds,
                "max_polls": self.max_polls or "unlimited",
                "change_detection": self.change_filter is not None,
            },
        )
        while not stop.is_set():
            started = time.monotonic()
            self._poll_once()
            self.stats.polls += 1

            if self.max_polls and self.stats.polls >= self.max_polls:
                log.info("max_polls reached", extra=self.stats.as_dict())
                break

            # Fixed rate, not fixed delay: a cycle taking 4 minutes must not turn an
            # hourly interval into a 64-minute one, or the loop drifts.
            delay = max(0.0, self.poll_interval_seconds - (time.monotonic() - started))
            if stop.wait(delay):
                break

        log.info("ingestion stopped", extra=self.stats.as_dict())
        return self.stats

    def _poll_once(self) -> None:
        try:
            result = self.source.poll()
        except TimetablesUnavailable as exc:
            # Expected and transient. The next cycle is the retry, and because fchg
            # carries a ~28h window, a missed cycle costs nothing.
            self.stats.failures += 1
            log.error(
                "poll failed",
                extra={"error": type(exc).__name__, "detail": str(exc),
                       "failures_total": self.stats.failures},
            )
            return

        records = result.observations
        if self.change_filter is not None:
            # Always runs, even for an empty poll: the filter has to record that the
            # scope is now empty, or departed trains stay remembered forever.
            selected = self.change_filter.select(records)
            self.change_filter.save()
        else:
            selected = records

        written = self.sink.write(selected) if selected else 0
        self.stats.observations_seen += len(records)
        self.stats.observations_written += written
        self.stats.observations_suppressed += len(records) - len(selected)

        if not records:
            log.warning("poll produced no in-scope observations", extra=result.counters())
            return
        log.info("poll ok", extra={**result.counters(), "written": written,
                                   "suppressed": len(records) - len(selected)})
