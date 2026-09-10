import json
import os
import sys

from kafka import KafkaConsumer

TOPIC = "railway.gtfs.trip_updates"

# Pass "earliest" as an argument to override the default.
reset = sys.argv[1] if len(sys.argv) > 1 else "latest"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=os.environ["KAFKA_USERNAME"],
    sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    ssl_cafile=os.environ["KAFKA_CA_CERT"],
    client_id="step4-consumer",
    group_id=None,
    auto_offset_reset=reset,
    consumer_timeout_ms=8000,
)

print(f"reading with auto_offset_reset={reset!r} ... (waits 8s then stops)\n")

count = 0
for record in consumer:
    count += 1
    value = json.loads(record.value)
    print(f"partition={record.partition} offset={record.offset} "
          f"key={record.key.decode()} trip={value['trip_id']} "
          f"stops={len(value['stop_time_updates'])}")

print(f"\n{count} records read")
consumer.close()