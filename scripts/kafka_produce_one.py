import json
import os

from kafka import KafkaProducer

TOPIC = "railway.gtfs.trip_updates"

producer = KafkaProducer(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=os.environ["KAFKA_USERNAME"],
    sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    ssl_cafile=os.environ["KAFKA_CA_CERT"],
    client_id="step3-producer",
)

message = {
    "trip_id": "740259",
    "start_date": "20260909",
    "stop_time_updates": [
        {"stop_sequence": 5, "stop_id": "S_HAGEN", "departure_delay": 0},
        {"stop_sequence": 6, "stop_id": "S_DORTMUND", "arrival_delay": 5400},
    ],
}

key = f"{message['trip_id']}:{message['start_date']}"

# Kafka takes bytes.
future = producer.send(
    TOPIC,
    key=key.encode("utf-8"),
    value=json.dumps(message).encode("utf-8"),
)

print("send() returned immediately - nothing is confirmed yet")

metadata = future.get(timeout=30)   # blocks until the broker acknowledges

print("broker confirmed:")
print(f"  topic      {metadata.topic}")
print(f"  partition  {metadata.partition}")
print(f"  offset     {metadata.offset}")
print(f"  key        {key}")

producer.flush()
producer.close()