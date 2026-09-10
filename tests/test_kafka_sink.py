"""Offline tests for the Kafka sink. No broker, no network."""

from __future__ import annotations

import json

from test_service import observation

from railway_network_analytics.kafka_sink import SCHEMA_VERSION, KafkaSink


class FakeProducer:
    """Records what would have been sent, and whether it was flushed."""

    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.flushes = 0
        self.closed = False

    def send(self, topic, key=None, value=None, headers=None):
        self.sent.append({"topic": topic, "key": key, "value": value, "headers": headers})

    def flush(self):
        self.flushes += 1

    def close(self):
        self.closed = True


def test_one_message_per_observation():
    producer = FakeProducer()
    sink = KafkaSink(producer, "railway.db.stop_observations")
    assert sink.write([observation("a-2609101000-1"), observation("b-2609101000-2")]) == 2
    assert [m["topic"] for m in producer.sent] == ["railway.db.stop_observations"] * 2


def test_key_is_the_stop_id():
    """stop_id is the natural key: one message per key, high cardinality, and it keeps
    a compacted 'latest state per stop' topic possible later."""
    producer = FakeProducer()
    KafkaSink(producer, "t").write([observation("1-2609101000-3")])
    assert producer.sent[0]["key"] == b"1-2609101000-3"


def test_value_is_the_full_record_as_json():
    producer = FakeProducer()
    KafkaSink(producer, "t").write([observation()])
    payload = json.loads(producer.sent[0]["value"])
    assert payload["train_category"] == "ICE"
    assert payload["train_number"] == "575"
    assert payload["planned_arrival"] == "2609101114"
    assert payload["observed_at"]  # ingestion time travels with the record


def test_key_and_value_are_bytes():
    """Kafka only knows bytes; JSON is our encoding choice, not something Kafka knows."""
    producer = FakeProducer()
    KafkaSink(producer, "t").write([observation()])
    assert isinstance(producer.sent[0]["key"], bytes)
    assert isinstance(producer.sent[0]["value"], bytes)


def test_schema_version_travels_as_a_header():
    """A migration handle that keeps the payload clean."""
    producer = FakeProducer()
    KafkaSink(producer, "t").write([observation()])
    assert producer.sent[0]["headers"] == [("schema_version", SCHEMA_VERSION.encode())]


def test_flush_is_called_once_per_write_not_per_message():
    """send() only buffers. One flush per cycle, not per record: flushing per message
    would cost a broker round trip each time."""
    producer = FakeProducer()
    KafkaSink(producer, "t").write([observation(f"{i}-2609101000-1") for i in range(50)])
    assert len(producer.sent) == 50
    assert producer.flushes == 1


def test_writing_nothing_still_flushes_but_sends_nothing():
    producer = FakeProducer()
    assert KafkaSink(producer, "t").write([]) == 0
    assert producer.sent == []


def test_close_closes_the_producer():
    producer = FakeProducer()
    KafkaSink(producer, "t").close()
    assert producer.closed
