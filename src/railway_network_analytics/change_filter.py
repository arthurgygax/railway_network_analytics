"""Emit only observations whose content differs from the previous cycle.

`fchg` returns every known change over a rolling ~28h window, so consecutive hourly
polls overlap heavily: most of what comes back was already emitted last cycle.

State is a short digest per stop id, persisted to disk. Persistence is not optional
here — an hourly ingestion is naturally a cron-style run that does not survive
between cycles, so in-memory state would suppress nothing.

Not source-specific: it works on anything exposing `key()` and `payload()`.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Protocol

log = logging.getLogger(__name__)


class Observation(Protocol):
    def key(self) -> str: ...
    def payload(self) -> tuple: ...


def digest(payload: tuple) -> str:
    """Short, stable content hash.

    Deliberately NOT builtin hash(): it is salted per process by PYTHONHASHSEED, so
    the same payload would digest differently across runs and suppress nothing.
    """
    encoded = json.dumps(payload, default=str, sort_keys=True).encode("utf-8")
    return hashlib.blake2b(encoded, digest_size=8).hexdigest()


class ChangeFilter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.seen: dict[str, str] = {}
        if path.is_file():
            try:
                self.seen = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                # A corrupt state file must not stop ingestion. Worst case we re-emit
                # one cycle, which downstream has to tolerate anyway.
                log.warning("unreadable change state, starting cold",
                            extra={"path": str(path), "detail": str(exc)})

    def select[T: Observation](self, observations: list[T]) -> list[T]:
        """Return only what changed, and adopt this cycle's state.

        The state is REPLACED, not merged: a stop that drops out of the feed is
        forgotten, which bounds the file to one cycle's worth of keys. The cost is
        that a stop reappearing unchanged is re-emitted once — at-least-once, which
        downstream must handle regardless.
        """
        current = {o.key(): digest(o.payload()) for o in observations}
        changed = [o for o in observations if self.seen.get(o.key()) != current[o.key()]]
        self.seen = current
        return changed

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.seen), encoding="utf-8")
        tmp.replace(self.path)  # atomic: a crash mid-write cannot corrupt the state
