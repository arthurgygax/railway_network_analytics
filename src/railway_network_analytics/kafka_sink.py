"""Kafka output boundary. Implements the same ObservationSink protocol as JsonLinesSink,
so swapping the destination touches nothing in `service.py`.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass

from kafka import KafkaProducer

from .config import Config
from .db_timetables import StopObservation

log = logging.getLogger(__name__)

SCHEMA_VERSION = "1"
FLUSH_TIMEOUT_SECONDS = 120


class KafkaDeliveryError(RuntimeError):
    """Records were sent but the broker never confirmed them."""


def build_producer(config: Config) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=config.kafka_bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=config.kafka_username,
        sasl_plain_password=config.kafka_password,
        ssl_cafile=str(config.kafka_ca_cert),
        client_id="railway-ingest",
        # Durability. These match kafka-python 3.x defaults today, but are stated
        # explicitly because a library upgrade must not silently change them.
        #
        # NOTE: acks="all" waits for all IN-SYNC replicas, and this cluster has
        # min.insync.replicas=1 — so today it is no stronger than acks=1.
        acks="all",
        enable_idempotence=True,
        # Measured ~14x on our payloads: consecutive observations repeat the same JSON
        # keys, so a batch compresses very well. gzip needs no extra dependency.
        compression_type="gzip",
        # Our data is already up to an hour stale; 20ms of batching costs nothing real.
        linger_ms=20,
    )


@dataclass
class KafkaSink:
    """Builds a fresh producer for every write.

    We publish once an hour, so a producer held open between cycles spends ~59 minutes
    idle. kafka-python's sender thread died in exactly that situation:

        File "kafka/producer/sender.py", line 460, in _complete_batch
            self._accumulator.muted.remove(batch.topic_partition)
        KeyError: TopicPartition(topic='railway.db.stop_observations', partition=0)

    A dead sender thread does not raise — sends simply never complete, so the service
    ran 18 hours and published one cycle. Creating the producer per cycle costs about a
    second an hour and removes the entire class of stale-connection-state bugs.
    """

    config: Config
    topic: str

    def write(self, observations: Iterable[StopObservation]) -> int:
        records = list(observations)
        if not records:
            return 0

        producer = build_producer(self.config)
        try:
            futures = []
            for observation in records:
                futures.append(producer.send(
                    self.topic,
                    # stop_id = {trip_id}-{start_datetime}-{stop_index}: the natural key,
                    # high cardinality, and one message per key — which keeps a compacted
                    # "latest state per stop" topic possible later.
                    key=observation.key().encode("utf-8"),
                    value=json.dumps(
                        observation.to_dict(), separators=(",", ":")
                    ).encode("utf-8"),
                    headers=[("schema_version", SCHEMA_VERSION.encode("utf-8"))],
                ))
            producer.flush(timeout=FLUSH_TIMEOUT_SECONDS)

            # flush() returning is NOT proof of delivery: it does not surface per-record
            # failures. Without this check `written` counts what we handed the buffer,
            # not what the broker stored.
            delivered = sum(1 for f in futures if f.succeeded())
            if delivered != len(records):
                failed = next((f for f in futures if not f.succeeded()), None)
                raise KafkaDeliveryError(
                    f"only {delivered}/{len(records)} records confirmed by the broker"
                    + (f": {failed.exception}" if failed is not None else "")
                )
            return delivered
        finally:
            producer.close(timeout=30)
