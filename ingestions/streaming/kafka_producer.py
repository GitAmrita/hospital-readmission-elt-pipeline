import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "encounters"


def publish_encounters(encounters: list[dict]):
    for encounter in encounters:
        producer.send(TOPIC, encounter)
    producer.flush()
    print(f"Published {len(encounters)} encounters to Kafka")