"""Offline tests for the Kafka→volume bridge. No broker, no Databricks."""

from __future__ import annotations

import contextlib
import gzip
import json
import threading
from dataclasses import dataclass

from railway_network_analytics import bridge
from railway_network_analytics.bridge import BridgeConfig, batch_name, drain, to_line


@dataclass
class Rec:
    topic: str = "railway.db.stop_observations"
    partition: int = 0
    offset: int = 1
    timestamp: int = 1789000000000
    key: bytes | None = b"1-2609101000-3"
    value: bytes = b'{"stop_id":"1-2609101000-3","train_number":"575"}'


class TP:
    def __init__(self, topic, partition):
        self.topic, self.partition = topic, partition

    def __hash__(self):
        return hash((self.topic, self.partition))


class FakeConsumer:
    def __init__(self, *polls):
        self.polls = list(polls)
        self.calls = 0
        self.commits = 0

    def poll(self, timeout_ms=0, max_records=None):
        self.calls += 1
        return self.polls[self.calls - 1] if self.calls <= len(self.polls) else {}

    def commit(self):
        self.commits += 1

    def close(self, timeout_ms=None):
        pass


class FakeFiles:
    def __init__(self, fail_on: str | None = None):
        self.uploaded: dict[str, bytes] = {}
        self.fail_on = fail_on

    def upload(self, file_path, contents, overwrite=None):
        if self.fail_on and self.fail_on in file_path:
            raise RuntimeError("upload failed")
        self.uploaded[file_path] = contents.read()


class FakeClient:
    def __init__(self, fail_on=None):
        self.files = FakeFiles(fail_on)


def config(**over) -> BridgeConfig:
    base = dict(
        bootstrap_servers="h:1", username="u", password="p", ca_cert="ca.pem",
        topic="railway.db.stop_observations", group_id="g", volume="/Volumes/v",
        batch_size=500, poll_timeout_ms=1, interval_seconds=0, max_batches=1,
        log_level="INFO", log_format="text",
    )
    return BridgeConfig(**{**base, **over})


# ------------------------------------------------------------------ serialisation


def test_line_mirrors_the_kafka_source_columns():
    """Same columns spark.readStream.format("kafka") produces, so Bronze would not
    change if Kafka ever became directly reachable."""
    line = json.loads(to_line(Rec()))
    assert set(line) == {"topic", "partition", "offset", "timestamp", "key", "value"}


def test_value_stays_an_unparsed_string():
    """Bronze's job is fidelity. Parsing belongs in Silver."""
    line = json.loads(to_line(Rec()))
    assert isinstance(line["value"], str)
    assert json.loads(line["value"])["train_number"] == "575"


def test_null_key_is_preserved_as_null():
    assert json.loads(to_line(Rec(key=None)))["key"] is None


def test_each_record_is_one_line():
    body = b"".join(to_line(Rec(offset=i)) for i in range(3))
    assert body.count(b"\n") == 3
    assert [json.loads(x)["offset"] for x in body.splitlines()] == [0, 1, 2]


# -------------------------------------------------------------------- file naming


def test_name_is_derived_from_offsets_not_the_clock():
    """Replaying the same offsets must produce the same filename, so a crash between
    upload and commit overwrites rather than duplicates."""
    assert batch_name("t", 0, 1450, 1599) == "t-p0-000000001450-000000001599.jsonl.gz"
    assert batch_name("t", 0, 1450, 1599) == batch_name("t", 0, 1450, 1599)


def test_name_is_zero_padded_so_it_sorts_lexically():
    assert batch_name("t", 0, 9, 10) < batch_name("t", 0, 100, 101)


def test_different_partitions_get_different_files():
    assert batch_name("t", 0, 1, 2) != batch_name("t", 1, 1, 2)


# --------------------------------------------------------------------- run loop


def test_batch_is_uploaded_then_committed():
    consumer = FakeConsumer({TP("t", 0): [Rec(offset=1), Rec(offset=2)]})
    client = FakeClient()
    stats = drain(consumer, client, config(), threading.Event())
    assert stats == {"batches": 1, "records": 2, "files": 1, "empty_polls": 0}
    name = "/Volumes/v/t-p0-000000000001-000000000002.jsonl.gz"
    assert list(client.files.uploaded) == [name]
    # Uploaded gzipped, since the uplink is the bottleneck. Spark's JSON reader
    # decompresses by extension, so Bronze still sees one JSON object per line.
    assert gzip.decompress(client.files.uploaded[name]).count(b"\n") == 2
    assert consumer.commits == 1


def test_each_partition_becomes_its_own_file():
    consumer = FakeConsumer({TP("t", 0): [Rec(partition=0, offset=5)],
                             TP("t", 1): [Rec(partition=1, offset=9)]})
    client = FakeClient()
    stats = drain(consumer, client, config(), threading.Event())
    assert stats["files"] == 2
    assert consumer.commits == 1  # one commit for the whole batch, after both uploads


def test_a_failed_upload_prevents_the_commit():
    """The reason auto-commit is off: offsets must not advance past data that never
    reached Databricks. Leaving them uncommitted makes the next run replay the batch."""
    consumer = FakeConsumer({TP("t", 0): [Rec(offset=1)]})
    client = FakeClient(fail_on="p0")
    with contextlib.suppress(RuntimeError):
        drain(consumer, client, config(), threading.Event())
    assert consumer.commits == 0


def test_one_empty_poll_does_not_end_the_drain():
    """The regression this exists for: kafka-python returns {} when the group join does
    not finish inside the poll timeout, which on a slow link is the normal first poll of
    a fresh consumer. Quitting on it skipped a whole hour of data with 'records: 0'."""
    consumer = FakeConsumer({}, {TP("t", 0): [Rec()]})
    stats = drain(consumer, FakeClient(), config(max_batches=1), threading.Event())
    assert stats["empty_polls"] == 1
    assert stats["records"] == 1  # the batch the first empty poll used to hide
    assert consumer.commits == 1


def test_a_streak_of_empty_polls_ends_the_drain():
    """Actually drained: stop, so the caller can sleep instead of holding a connection."""
    consumer = FakeConsumer({})
    stats = drain(consumer, FakeClient(), config(), threading.Event())
    assert consumer.calls == bridge.EMPTY_POLLS_TO_DRAIN
    assert stats["batches"] == 0
    assert consumer.commits == 0


def test_stop_event_ends_the_loop_without_polling():
    consumer = FakeConsumer({TP("t", 0): [Rec()]})
    stop = threading.Event()
    stop.set()
    stats = drain(consumer, FakeClient(), config(), stop)
    assert consumer.calls == 0
    assert stats["batches"] == 0


def test_a_failed_drain_waits_for_the_next_interval_instead_of_crashing():
    """A dead uplink used to kill the process, and `restart: unless-stopped` put us
    straight back on the same dead uplink with no hour of backoff. Offsets stay
    uncommitted either way, so the next drain replays the batch."""
    stop = threading.Event()

    class OneShot(FakeConsumer):
        def close(self, timeout_ms=None):
            stop.set()  # end the hourly loop after this drain

    consumer = OneShot({TP("t", 0): [Rec()]})
    original = bridge.build_consumer
    bridge.build_consumer = lambda _c: consumer
    try:
        stats = bridge.run(FakeClient(fail_on="p0"), config(), stop)
    finally:
        bridge.build_consumer = original

    assert stats["drains"] == 1  # survived the failure and completed the cycle
    assert consumer.commits == 0


def test_run_closes_the_consumer_between_hourly_drains():
    """Sleeping an hour inside a live consumer group is what produced

        CommitFailedError: ... the consumer was kicked out of the group

    because the coordinator expects a poll every max.poll.interval.ms. Leave the group
    and rejoin instead — offsets live on the broker, so nothing is lost."""
    built, closed = [], []

    class TrackedConsumer(FakeConsumer):
        def close(self, timeout_ms=None):
            closed.append(self)

    def fake_build(_config):
        c = TrackedConsumer({TP("t", 0): [Rec()]})
        built.append(c)
        return c

    original = bridge.build_consumer
    bridge.build_consumer = fake_build
    try:
        stats = bridge.run(FakeClient(), config(max_batches=1), threading.Event())
    finally:
        bridge.build_consumer = original

    assert len(built) == 1
    assert closed == built          # closed before sleeping, never held idle
    assert stats["drains"] == 1


def test_run_stops_immediately_on_shutdown_rather_than_sleeping_an_hour():
    original = bridge.build_consumer
    bridge.build_consumer = lambda _c: FakeConsumer({})
    stop = threading.Event()
    stop.set()
    try:
        stats = bridge.run(FakeClient(), config(interval_seconds=3600), stop)
    finally:
        bridge.build_consumer = original
    assert stats["drains"] == 0
