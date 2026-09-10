"""Bridge: Aiven Kafka -> JSONL files -> Databricks Unity Catalog volume.

Databricks Free Edition blocks outbound network access, so Databricks cannot pull
from Kafka. This process pushes instead: it consumes the topic and uploads batches to
a volume, which Databricks reads with a file-source stream.

Kafka stays a real 3-day buffer between two independently failing processes — if this
bridge is down, nothing is lost until retention expires.

    set -a; source .env; set +a; uv run python -m railway_network_analytics.bridge
"""

from __future__ import annotations

import io
import json
import logging
import os
import signal
import sys
import threading
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from kafka import KafkaConsumer

from .logging_setup import configure_logging

log = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_CONFIG = 2


@dataclass(frozen=True, slots=True)
class BridgeConfig:
    """Separate from the ingestion service's Config: different program, different env."""

    bootstrap_servers: str
    username: str
    password: str
    ca_cert: str
    topic: str
    group_id: str
    volume: str
    batch_size: int
    poll_timeout_ms: int
    max_batches: int
    log_level: str
    log_format: str

    @classmethod
    def from_env(cls) -> BridgeConfig:
        missing = [
            k for k in ("KAFKA_BOOTSTRAP_SERVERS", "KAFKA_USERNAME", "KAFKA_PASSWORD",
                        "DATABRICKS_HOST", "DATABRICKS_TOKEN")
            if not os.environ.get(k)
        ]
        if missing:
            raise ValueError("missing required environment variables: " + ", ".join(missing))
        return cls(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            username=os.environ["KAFKA_USERNAME"],
            password=os.environ["KAFKA_PASSWORD"],
            ca_cert=os.environ.get("KAFKA_CA_CERT", "certs/ca.pem"),
            topic=os.environ.get("KAFKA_TOPIC", "railway.db.stop_observations"),
            group_id=os.environ.get("BRIDGE_GROUP_ID", "railway-bridge"),
            volume=os.environ.get("DATABRICKS_VOLUME", "/Volumes/railway/raw/landing"),
            batch_size=int(os.environ.get("BRIDGE_BATCH_SIZE", "500")),
            poll_timeout_ms=int(os.environ.get("BRIDGE_POLL_TIMEOUT_MS", "10000")),
            max_batches=int(os.environ.get("BRIDGE_MAX_BATCHES", "0")),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
            log_format=os.environ.get("LOG_FORMAT", "text"),
        )


def build_consumer(config: BridgeConfig) -> KafkaConsumer:
    return KafkaConsumer(
        config.topic,
        bootstrap_servers=config.bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=config.username,
        sasl_plain_password=config.password,
        ssl_cafile=config.ca_cert,
        group_id=config.group_id,
        client_id="railway-bridge",
        auto_offset_reset="earliest",
        # The whole point: commit only after the upload succeeded. Auto-commit runs on
        # a timer and would mark messages done before they reached Databricks.
        enable_auto_commit=False,
        max_poll_records=config.batch_size,
    )


def to_line(record) -> bytes:
    """One JSONL line per Kafka record.

    Deliberately the same columns `spark.readStream.format("kafka")` produces, with the
    value left as an unparsed string. Bronze's job is fidelity, not interpretation —
    and this keeps Bronze identical if Kafka ever becomes directly reachable.
    """
    return json.dumps({
        "topic": record.topic,
        "partition": record.partition,
        "offset": record.offset,
        "timestamp": record.timestamp,
        "key": record.key.decode("utf-8") if record.key else None,
        "value": record.value.decode("utf-8"),
    }, separators=(",", ":")).encode("utf-8") + b"\n"


def batch_name(topic: str, partition: int, first: int, last: int) -> str:
    """Derived from offsets, never the clock.

    A crash between upload and commit means the next run re-reads the same offsets and
    writes this same name — an overwrite rather than a duplicate. That is what turns
    at-least-once delivery into effectively-once storage.
    """
    return f"{topic}-p{partition}-{first:012d}-{last:012d}.jsonl"


def run(consumer: KafkaConsumer, client: WorkspaceClient, config: BridgeConfig,
        stop: threading.Event) -> dict[str, int]:
    stats = {"batches": 0, "records": 0, "files": 0, "empty_polls": 0}
    log.info("bridge starting", extra={"topic": config.topic, "group": config.group_id,
                                       "volume": config.volume,
                                       "max_batches": config.max_batches or "unlimited"})
    while not stop.is_set():
        polled = consumer.poll(timeout_ms=config.poll_timeout_ms, max_records=config.batch_size)
        if not polled:
            stats["empty_polls"] += 1
            continue

        # Upload every partition BEFORE committing anything. A partial failure leaves
        # the offsets uncommitted, so the next run replays the whole batch.
        for tp, records in polled.items():
            body = b"".join(to_line(r) for r in records)
            name = batch_name(tp.topic, tp.partition, records[0].offset, records[-1].offset)
            client.files.upload(f"{config.volume}/{name}", io.BytesIO(body), overwrite=True)
            stats["files"] += 1
            stats["records"] += len(records)
            log.info("uploaded batch", extra={"file": name, "records": len(records),
                                              "bytes": len(body)})

        consumer.commit()
        stats["batches"] += 1
        if config.max_batches and stats["batches"] >= config.max_batches:
            log.info("max_batches reached", extra=stats)
            break

    log.info("bridge stopped", extra=stats)
    return stats


def main() -> int:
    try:
        config = BridgeConfig.from_env()
    except ValueError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return EXIT_CONFIG

    configure_logging(config.log_level, config.log_format)

    stop = threading.Event()

    def handle(signum: int, _frame: object) -> None:
        log.info("shutdown signal received", extra={"signal": signal.Signals(signum).name})
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handle)

    consumer = build_consumer(config)
    try:
        run(consumer, WorkspaceClient(), config, stop)
    finally:
        consumer.close()
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
