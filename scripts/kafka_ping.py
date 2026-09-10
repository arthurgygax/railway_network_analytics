import os

from kafka import KafkaAdminClient

admin = KafkaAdminClient(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=os.environ["KAFKA_USERNAME"],
    sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    ssl_cafile=os.environ["KAFKA_CA_CERT"],
    client_id="kafka-ping",
)

print("connected to:", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
print("brokers:")
for broker in admin.describe_cluster()["brokers"]:
    print(f"  broker_id={broker['broker_id']}  {broker['host']}:{broker['port']}")
print("topics:", sorted(admin.list_topics()))
admin.close()