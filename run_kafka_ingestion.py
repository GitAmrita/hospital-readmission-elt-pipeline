from fhir_api.fhir_api_helper import get_random_encounters_from_csv
from ingestions.streaming.kafka_producer import publish_encounters
from ingestions.streaming.kafka_consumer import consume_and_insert
import threading
import time


def run_kafka_pipeline():
    # Start consumer first so we do not miss early messages
    print("Starting Kafka consumer...")
    consumer_thread = threading.Thread(target=consume_and_insert, daemon=True)
    consumer_thread.start()

    print("Publishing encounters to Kafka...")
    encounters = get_random_encounters_from_csv(limit=100)
    publish_encounters(encounters)

    try:
        # Keep the process alive while consumer runs
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping Kafka pipeline.")

# if __name__ == "__main__":
#     run_kafka_pipeline()