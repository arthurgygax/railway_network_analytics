"""Entrypoint: `python -m railway_network_analytics` (or the `railway-ingest` script).

The only place that reads the environment, configures logging, installs signal
handlers, and wires the components together.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import signal
import sys
import threading

from .change_filter import ChangeFilter
from .config import Config, ConfigError
from .db_timetables import DbTimetablesSource, PlanCache, TimetablesClient
from .logging_setup import configure_logging
from .service import IngestionService
from .sink import JsonLinesSink, ObservationSink

log = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_CONFIG = 2
EXIT_TARGETS = 3
EXIT_SINK = 4

SECRET_HINTS = ("password", "secret", "token", "key")


def _redact(config: Config) -> dict[str, str]:
    """Redact by field-name hint, so a future secret is hidden by default.

    `kafka_ca_cert` is a path to a public certificate, not a secret — but it matches
    "cert" not the hints above, so it stays visible, which is what we want.
    """
    return {
        name: ("***" if any(hint in name for hint in SECRET_HINTS) else str(value))
        for name, value in dataclasses.asdict(config).items()
    }


def _install_signal_handlers(stop: threading.Event) -> None:
    def handle(signum: int, _frame: object) -> None:
        log.info("shutdown signal received", extra={"signal": signal.Signals(signum).name})
        stop.set()

    # SIGTERM is what `docker stop` sends; without handling it the container is
    # SIGKILLed after the grace period and the in-flight cycle is lost.
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handle)


def main() -> int:
    try:
        config = Config.from_env()
    except ConfigError as exc:
        # Logging is not configured yet, and a config error must stay visible even
        # when log routing itself is what is misconfigured.
        print(f"FATAL: {exc}", file=sys.stderr)
        return EXIT_CONFIG

    configure_logging(config.log_level, config.log_format)
    log.info("configuration loaded", extra=_redact(config))

    try:
        targets = json.loads(config.poll_targets_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log.critical("cannot read poll targets", extra={"detail": str(exc)})
        return EXIT_TARGETS
    if not targets:
        log.critical("poll targets file is empty — nothing would be ingested")
        return EXIT_TARGETS
    log.info("poll targets loaded", extra={"stations": len(targets)})

    sink: ObservationSink
    if config.sink == "kafka":
        # Imported lazily so a jsonl-only run never needs the kafka package loaded.
        from .kafka_sink import KafkaSink, build_producer

        sink = KafkaSink(build_producer(config), config.kafka_topic)
        log.info("using kafka sink", extra={"topic": config.kafka_topic})
    else:
        jsonl = JsonLinesSink(config.output_dir)
        try:
            jsonl.ensure_writable()
        except OSError as exc:
            log.critical(
                "output directory is not writable",
                extra={"output_dir": str(config.output_dir), "detail": str(exc),
                       "hint": "bind-mounted host dir? run with --user \"$(id -u):$(id -g)\" "
                               "or use a named volume"},
            )
            return EXIT_SINK
        sink = jsonl
        log.info("using jsonl sink", extra={"output_dir": str(config.output_dir)})

    service = IngestionService(
        source=DbTimetablesSource(
            client=TimetablesClient(config.timetables_client_id, config.timetables_api_key),
            targets=targets,
            cache=PlanCache(config.state_dir / "plan_cache.json"),
        ),
        sink=sink,
        poll_interval_seconds=config.poll_interval_seconds,
        max_polls=config.max_polls,
        change_filter=ChangeFilter(config.state_dir / "seen.json"),
    )

    stop = threading.Event()
    _install_signal_handlers(stop)
    try:
        service.run(stop)
    except OSError as exc:
        # Not transient like an API blip: if the sink breaks mid-run, looping on would
        # silently discard data. Fail loudly with a structured log, not a traceback.
        log.critical("sink write failed, stopping", extra={"detail": str(exc)})
        return EXIT_SINK
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
