"""Kafka output boundary. Implements the same ObservationSink protocol as JsonLinesSink."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass

from kafka import KafkaProducer

from .config import Config
from .db_timetables import StopObservation

log = logging.getLogger(__name__)


def build_producer(config: Config) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=config.kafka_bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=config.kafka_username,
        sasl_plain_password=config.kafka_password,
        ssl_cafile=config.kafka_ca_cert,
        client_id="railway-ingest",
    )


@dataclass
class KafkaSink:
    producer: KafkaProducer
    topic: str

    def write(self, trips: Iterable[StopObservation]) -> int:
        count = 0
        for trip in trips:
            self.producer.send(
                self.topic,
                key=trip.key().encode("utf-8"),
                value=json.dumps(trip.to_dict(), separators=(",", ":")).encode("utf-8"),
            )
            count += 1
        # One flush per poll: send() only buffers. Until this returns, nothing is durable.
        self.producer.flush()
        return count

    def close(self) -> None:
        self.producer.close()