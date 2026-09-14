"""Offline tests for the Kafka sink. No broker, no network."""

from __future__ import annotations

import json

import pytest
from test_service import observation

from railway_network_analytics.kafka_sink import (
    SCHEMA_VERSION,
    KafkaDeliveryError,
    KafkaSink,
)


class FakeFuture:
    def __init__(self, ok: bool = True, exception: str | None = None) -> None:
        self._ok = ok
        self.exception = exception

    def succeeded(self) -> bool:
        return self._ok


class FakeProducer:
    """Records what would have been sent, and whether it was flushed and closed."""

    def __init__(self, fail_after: int | None = None) -> None:
        self.sent: list[dict] = []
        self.flushes = 0
        self.closed = False
        self.fail_after = fail_after

    def send(self, topic, key=None, value=None, headers=None):
        self.sent.append({"topic": topic, "key": key, "value": value, "headers": headers})
        ok = self.fail_after is None or len(self.sent) <= self.fail_after
        return FakeFuture(ok, None if ok else "NotLeaderForPartition")

    def flush(self, timeout=None):
        self.flushes += 1

    def close(self, timeout=None):
        self.closed = True


@pytest.fixture
def sink(monkeypatch):
    """A KafkaSink whose build_producer is stubbed. Returns (sink, producers_created)."""
    created: list[FakeProducer] = []

    def make(fail_after=None):
        def _build(_config):
            p = FakeProducer(fail_after)
            created.append(p)
            return p

        monkeypatch.setattr("railway_network_analytics.kafka_sink.build_producer", _build)
        return KafkaSink(config=None, topic="railway.db.stop_observations"), created

    return make


def test_one_message_per_observation(sink):
    s, created = sink()
    assert s.write([observation("a-2609101000-1"), observation("b-2609101000-2")]) == 2
    assert [m["topic"] for m in created[0].sent] == ["railway.db.stop_observations"] * 2


def test_key_is_the_stop_id(sink):
    s, created = sink()
    s.write([observation("1-2609101000-3")])
    assert created[0].sent[0]["key"] == b"1-2609101000-3"


def test_value_is_the_full_record_as_json(sink):
    s, created = sink()
    s.write([observation()])
    payload = json.loads(created[0].sent[0]["value"])
    assert payload["train_category"] == "ICE"
    assert payload["train_number"] == "575"
    assert payload["observed_at"]


def test_key_and_value_are_bytes(sink):
    """Kafka only knows bytes; JSON is our encoding choice, not something Kafka knows."""
    s, created = sink()
    s.write([observation()])
    assert isinstance(created[0].sent[0]["key"], bytes)
    assert isinstance(created[0].sent[0]["value"], bytes)


def test_schema_version_travels_as_a_header(sink):
    s, created = sink()
    s.write([observation()])
    assert created[0].sent[0]["headers"] == [("schema_version", SCHEMA_VERSION.encode())]


def test_flush_once_per_write_not_per_message(sink):
    s, created = sink()
    s.write([observation(f"{i}-2609101000-1") for i in range(50)])
    assert len(created[0].sent) == 50
    assert created[0].flushes == 1


def test_a_fresh_producer_per_write(sink):
    """kafka-python's sender thread died during the ~59 idle minutes between hourly
    cycles (KeyError in _complete_batch), and a dead sender does not raise — the
    service ran 18 hours and published one cycle. Never hold a producer across cycles."""
    s, created = sink()
    s.write([observation("a-2609101000-1")])
    s.write([observation("b-2609101000-1")])
    assert len(created) == 2
    assert all(p.closed for p in created)


def test_producer_is_closed_even_when_delivery_fails(sink):
    s, created = sink(fail_after=0)
    with pytest.raises(KafkaDeliveryError):
        s.write([observation()])
    assert created[0].closed


def test_unconfirmed_records_raise_instead_of_being_counted(sink):
    """flush() returning is NOT proof of delivery — it does not surface per-record
    failures. Without checking the futures, `written` counts what we handed the buffer
    rather than what the broker stored."""
    s, _ = sink(fail_after=2)
    with pytest.raises(KafkaDeliveryError, match="only 2/5 records confirmed"):
        s.write([observation(f"{i}-2609101000-1") for i in range(5)])


def test_writing_nothing_builds_no_producer(sink):
    s, created = sink()
    assert s.write([]) == 0
    assert created == []
