import json
import os

from kafka import KafkaConsumer, TopicPartition

TOPIC = "railway.gtfs.trip_updates"
GROUP = "railway-tutorial"
TP = TopicPartition(TOPIC, 0)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=os.environ["KAFKA_USERNAME"],
    sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    ssl_cafile=os.environ["KAFKA_CA_CERT"],
    client_id="step5-consumer",
    group_id=GROUP, # THE CHANGE
    auto_offset_reset="earliest",
    consumer_timeout_ms=8000,
)

end = consumer.end_offsets([TP])[TP]
committed = consumer.committed(TP)
print(f"group          {GROUP}")
print(f"log end offset {end}          <- next offset the producer will write")
print(f"committed      {committed}    <- where this group left off")
print(f"lag            {end - (committed or 0)}\n")

count = 0
for record in consumer:
    count += 1
    value = json.loads(record.value)
    print(f"  offset={record.offset} key={record.key.decode()} trip={value['trip_id']}")

print(f"\n{count} records read")
print(f"committed now  {consumer.committed(TP)}")
consumer.close()