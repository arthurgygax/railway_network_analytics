import json
import os

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

TOPIC = "railway.gtfs.trip_updates"

admin = KafkaAdminClient(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=os.environ["KAFKA_USERNAME"],
    sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    ssl_cafile=os.environ["KAFKA_CA_CERT"],
    client_id="kafka-admin",
)

try:
    admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=2)])
    print(f"created {TOPIC}")
except TopicAlreadyExistsError:
    print(f"{TOPIC} already exists, this script is safe to re-run")

print("\ntopics now:", sorted(admin.list_topics()))
print("\ndescription:")
print(json.dumps(admin.describe_topics([TOPIC]), indent=2))
admin.close()