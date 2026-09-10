"""Create and configure the stop-observation topic, then describe it back.

Idempotent: safe to re-run. Pass --cleanup to also delete the tutorial topics.

    set -a; source .env; set +a; uv run python scripts/kafka_create_topic.py
"""

from __future__ import annotations

import os
import sys

from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

TOPIC = "railway.db.stop_observations"
TUTORIAL_TOPICS = ["db-test", "railway.gtfs.trip_updates", "railway.gtfs.key_demo"]

# 2 partitions is this Aiven plan's cap for a user topic, and at ~3 messages/second
# it is far more than throughput needs. Partition count can only ever be increased,
# and increasing it rehashes keys, so it is not a knob to fiddle with.
PARTITIONS = 2
REPLICATION = 2  # the cluster has exactly 2 brokers

CONFIGS = {
    # We wanted 7 days of replay. This Aiven plan REFUSES it:
    #   PolicyViolationError: Configuration 'retention.ms' violates policy
    # so retention stays at the plan's 3 days and must be changed (if at all) in the
    # Aiven console, not through Kafka's admin API. Attempt it anyway so the refusal
    # is visible rather than assumed.
    "retention.ms": str(7 * 24 * 60 * 60 * 1000),
    # `delete`, not `compact`: Bronze needs the history. Compaction keeps only the
    # latest message per key, which would annihilate exactly the delay evolution we
    # are collecting this data to study. Already the broker-level default here.
    "cleanup.policy": "delete",
}

SHOW = ["min.insync.replicas", "retention.ms", "cleanup.policy",
        "compression.type", "max.message.bytes"]

admin = KafkaAdminClient(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=os.environ["KAFKA_USERNAME"],
    sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    ssl_cafile=os.environ["KAFKA_CA_CERT"],
    client_id="railway-admin",
)

try:
    admin.create_topics([NewTopic(TOPIC, PARTITIONS, REPLICATION)])
    print(f"created {TOPIC} ({PARTITIONS} partitions, replication {REPLICATION})")
except TopicAlreadyExistsError:
    print(f"{TOPIC} already exists")

# alter_configs reports failures INSIDE the response, not by raising. Ignoring the
# return value makes the script claim success it did not have.
for name, value in CONFIGS.items():
    response = admin.alter_configs(
        [ConfigResource(ConfigResourceType.TOPIC, TOPIC, configs={name: value})]
    )
    # A successful alter reports the string "OK"; a refusal reports the error.
    outcome = response.get("topic", {}).get(TOPIC)
    if outcome in (None, "", "OK"):
        print(f"  applied  {name}={value}")
    else:
        print(f"  REFUSED  {name}={value}: {outcome}")

if "--cleanup" in sys.argv:
    for name in TUTORIAL_TOPICS:
        try:
            admin.delete_topics([name])
            print(f"deleted tutorial topic {name}")
        except UnknownTopicOrPartitionError:
            pass

cfg = admin.describe_configs(
    [ConfigResource(ConfigResourceType.TOPIC, TOPIC)], config_filter="all"
)["topic"][TOPIC]
print(f"\n{TOPIC} config:")
for name in SHOW:
    entry = cfg.get(name)
    if entry:
        print(f"  {name:22} {entry['value']:<16} ({entry['config_source']})")

description = admin.describe_topics([TOPIC])[0]
print("\npartitions:")
for part in description["partitions"]:
    print(f"  partition {part['partition_index']}  leader={part['leader_id']}  "
          f"replicas={part['replica_nodes']}  isr={part['isr_nodes']}")

print("\ntopics now:", sorted(admin.list_topics()))
admin.close()
