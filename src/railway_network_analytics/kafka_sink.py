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
        # min.insync.replicas=1 — so today it is no stronger than acks=1. Raising that
        # to 2 would make it a real guarantee at the cost of halting writes whenever
        # one of the two brokers is down.
        acks="all",
        enable_idempotence=True,
        # Measured ~14x on our payloads: consecutive observations repeat the same JSON
        # keys, so a batch compresses very well. gzip is the only codec available
        # without an extra dependency.
        compression_type="gzip",
        # Our data is already up to an hour stale; 20ms of batching costs nothing real.
        linger_ms=20,
    )


@dataclass
class KafkaSink:
    producer: KafkaProducer
    topic: str

    def write(self, observations: Iterable[StopObservation]) -> int:
        count = 0
        for observation in observations:
            self.producer.send(
                self.topic,
                # stop_id = {trip_id}-{start_datetime}-{stop_index}: the natural key of
                # the record, high cardinality, and one message per key — which keeps a
                # compacted "latest state per stop" topic possible later.
                key=observation.key().encode("utf-8"),
                value=json.dumps(observation.to_dict(), separators=(",", ":")).encode("utf-8"),
                # Cheap migration handle: a consumer can route or reject by version
                # without the payload having to carry it.
                headers=[("schema_version", SCHEMA_VERSION.encode("utf-8"))],
            )
            count += 1
        # One flush per cycle. send() only buffers — until this returns, nothing is
        # durable. It does not surface per-record failures; see `write` in the README.
        self.producer.flush()
        return count

    def close(self) -> None:
        self.producer.close()
