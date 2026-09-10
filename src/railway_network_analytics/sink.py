"""Output boundary.

Phase 3 writes JSON Lines to disk. Phase 5 replaces this with a Kafka producer that
implements the same `ObservationSink` protocol; nothing in `service.py` changes.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from .db_timetables import StopObservation

log = logging.getLogger(__name__)


class ObservationSink(Protocol):
    def write(self, observations: Iterable[StopObservation]) -> int: ...


@dataclass
class JsonLinesSink:
    """Append-only JSONL, one file per UTC day.

    Opened and closed per write: the file handle never outlives a poll, so day
    rollover is handled for free and a crash cannot leave a half-flushed buffer
    holding records that the logs claim were written.
    """

    output_dir: Path

    def path_for(self, moment: datetime) -> Path:
        return self.output_dir / f"observations-{moment:%Y-%m-%d}.jsonl"

    def ensure_writable(self) -> None:
        """Fail at startup, not after a 42 MB download.

        Without this, an unwritable output directory (the usual cause: a bind-mounted
        host directory whose uid does not match the container user) is only discovered
        at the end of the first poll — a minute of work thrown away, and the traceback
        blames the write rather than the setup.
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)
        probe = self.output_dir / ".write-probe"
        probe.touch()
        probe.unlink()

    def write(self, observations: Iterable[StopObservation]) -> int:
        records = [obs.to_dict() for obs in observations]
        if not records:
            return 0
        self.output_dir.mkdir(parents=True, exist_ok=True)
        path = self.path_for(datetime.now(UTC))
        with path.open("a", encoding="utf-8") as fh:
            for record in records:
                fh.write(json.dumps(record, separators=(",", ":")) + "\n")
        log.debug("wrote observations", extra={"path": str(path), "records": len(records)})
        return len(records)
