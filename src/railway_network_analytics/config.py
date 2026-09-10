"""Application configuration.

Single place where the process environment is read. Everything downstream takes a
`Config` instance; nothing else in the package touches `os.environ`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULTS = {
    "POLL_TARGETS_PATH": "data/raw/stada/poll_targets.json",
    "STATE_DIR": "data/state",
    "TIMETABLES_CLIENT_ID": "",
    "TIMETABLES_API_KEY": "",
    "OUTPUT_DIR": "data/out",
    "POLL_INTERVAL_SECONDS": "3600",
    "HTTP_TIMEOUT_SECONDS": "60",
    "MAX_POLLS": "0",
    "LOG_LEVEL": "INFO",
    "LOG_FORMAT": "text",
    "SINK": "jsonl",
    "KAFKA_TOPIC": "railway.gtfs.trip_updates",
    "KAFKA_BOOTSTRAP_SERVERS": "",
    "KAFKA_USERNAME": "",
    "KAFKA_PASSWORD": "",
    "KAFKA_CA_CERT": "certs/ca.pem",
}

LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
LOG_FORMATS = frozenset({"text", "json"})


class ConfigError(ValueError):
    """Raised when the environment does not describe a runnable configuration."""


@dataclass(frozen=True, slots=True)
class Config:
    poll_targets_path: Path
    state_dir: Path
    timetables_client_id: str
    timetables_api_key: str
    output_dir: Path
    poll_interval_seconds: int
    http_timeout_seconds: int
    max_polls: int
    log_level: str
    log_format: str
    sink: str
    kafka_topic: str
    kafka_bootstrap_servers: str
    kafka_username: str
    kafka_password: str
    kafka_ca_cert: str

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Config:
        """Build a validated Config. Raises ConfigError listing every problem at once."""
        src = os.environ if env is None else env
        errors: list[str] = []

        def get(key: str) -> str:
            """Empty string is treated as unset — Docker/compose pass empty vars readily."""
            return (src.get(key) or DEFAULTS[key]).strip()

        def positive_int(key: str, *, allow_zero: bool = False) -> int:
            raw = get(key)
            try:
                value = int(raw)
            except ValueError:
                errors.append(f"{key}={raw!r} is not an integer")
                return 0
            if value < 0 or (value == 0 and not allow_zero):
                errors.append(f"{key}={value} must be {'>= 0' if allow_zero else '> 0'}")
            return value

        targets_path = Path(get("POLL_TARGETS_PATH"))
        if not targets_path.is_file():
            errors.append(
                f"POLL_TARGETS_PATH={targets_path} does not exist. "
                "Run scripts/fetch_stada.py then scripts/pick_poll_targets.py."
            )

        for key in ("TIMETABLES_CLIENT_ID", "TIMETABLES_API_KEY"):
            if not get(key):
                errors.append(f"{key} is required")

        poll = positive_int("POLL_INTERVAL_SECONDS")
        timeout = positive_int("HTTP_TIMEOUT_SECONDS")
        max_polls = positive_int("MAX_POLLS", allow_zero=True)

        log_level = get("LOG_LEVEL").upper()
        if log_level not in LOG_LEVELS:
            errors.append(f"LOG_LEVEL={log_level!r} must be one of {sorted(LOG_LEVELS)}")

        log_format = get("LOG_FORMAT").lower()
        if log_format not in LOG_FORMATS:
            errors.append(f"LOG_FORMAT={log_format!r} must be one of {sorted(LOG_FORMATS)}")

        sink = get("SINK").lower()
        if sink not in {"jsonl", "kafka"}:
            errors.append(f"SINK={sink!r} must be 'jsonl' or 'kafka'")

        # Only required for the sink actually selected — otherwise offline runs
        # would demand credentials they never use.
        if sink == "kafka":
            for key in ("KAFKA_BOOTSTRAP_SERVERS", "KAFKA_USERNAME", "KAFKA_PASSWORD"):
                if not get(key):
                    errors.append(f"{key} is required when SINK=kafka")
            if not Path(get("KAFKA_CA_CERT")).is_file():
                errors.append(f"KAFKA_CA_CERT={get('KAFKA_CA_CERT')} does not exist")

        if errors:
            raise ConfigError("invalid configuration:\n  - " + "\n  - ".join(errors))

        return cls(
            poll_targets_path=targets_path,
            state_dir=Path(get("STATE_DIR")),
            timetables_client_id=get("TIMETABLES_CLIENT_ID"),
            timetables_api_key=get("TIMETABLES_API_KEY"),
            output_dir=Path(get("OUTPUT_DIR")),
            poll_interval_seconds=poll,
            http_timeout_seconds=timeout,
            max_polls=max_polls,
            log_level=log_level,
            log_format=log_format,
            sink=sink,
            kafka_topic=get("KAFKA_TOPIC"),
            kafka_bootstrap_servers=get("KAFKA_BOOTSTRAP_SERVERS"),
            kafka_username=get("KAFKA_USERNAME"),
            kafka_password=get("KAFKA_PASSWORD"),
            kafka_ca_cert=get("KAFKA_CA_CERT"),
        )
