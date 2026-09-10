"""Deterministic fixtures. No network, no wall clock, no credentials."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def poll_targets(tmp_path: Path):
    """Write a poll-targets file, the app's required reference input."""

    def build(targets=None) -> Path:
        path = tmp_path / "poll_targets.json"
        path.write_text(json.dumps(
            targets if targets is not None
            else [{"eva": 8000105, "name": "Frankfurt(Main)Hbf"}]
        ), encoding="utf-8")
        return path

    return build
