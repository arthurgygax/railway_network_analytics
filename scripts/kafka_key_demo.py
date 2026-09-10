import collections
import os

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

TOPIC = "railway.gtfs.key_demo"

conn = dict(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=os.environ["KAFKA_USERNAME"],
    sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    ssl_cafile=os.environ["KAFKA_CA_CERT"],
)

admin = KafkaAdminClient(**conn)
try:
    admin.create_topics([NewTopic(TOPIC, num_partitions=2, replication_factor=2)])
    print(f"created {TOPIC} with 2 partitions\n")
except TopicAlreadyExistsError:
    print(f"{TOPIC} already exists\n")
admin.close()

producer = KafkaProducer(**conn)

# Real trip_ids from our own feed.
print("A) six different trip keys:")
for trip in ["740259", "525218", "1598427", "1015619", "1035838", "1089617"]:
    key = f"{trip}:20260909"
    meta = producer.send(TOPIC, key=key.encode(), value=b"{}").get(timeout=30)
    print(f"     key={key:22} -> partition {meta.partition}")

print("\nB) the same key, three times:")
for i in range(3):
    meta = producer.send(TOPIC, key=b"740259:20260909", value=b"{}").get(timeout=30)
    print(f"     send {i + 1}                        -> partition {meta.partition}")

print("\nC) no key at all, eight times:")
spread = collections.Counter()
for _ in range(8):
    spread[producer.send(TOPIC, key=None, value=b"{}").get(timeout=30).partition] += 1
print(f"     partitions {dict(sorted(spread.items()))}")

producer.flush()
producer.close()