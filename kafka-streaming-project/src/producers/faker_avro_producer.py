import json
import time
import logging
from uuid import uuid4
from faker import Faker
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from datetime import datetime
import random


import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SCHEMA_PATH = os.path.join(BASE_DIR, "schemas", "input_event.avsc")


fake = Faker()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s"
)

SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_NAME = "email_marketing_events"

def load_avro_schema(schema_path: str):
    """Load Avro schema from file"""
    try:
        with open(schema_path, "r") as schema_file:
            return json.load(schema_file)
    except Exception as e:
        logging.error(f"Failed to load schema: {e}")
        return None

def generate_event():
    """Generate one synthetic email marketing event"""
    event_types = ["SENT", "DELIVERED", "OPENED", "CLICKED", "BOUNCED", "UNSUBSCRIBED"]

    return {
        "event_id": str(uuid4()),
        "event_timestamp": int(datetime.utcnow().timestamp() * 1000),
        "user_id": str(uuid4()),
        "email": fake.email(),
        "campaign_id": f"campaign_{random.randint(1, 5)}",
        "event_type": random.choice(event_types),
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "location": fake.country(),
        "spam_score": round(random.uniform(0, 1), 2),
        "engagement_score": round(random.uniform(0, 5), 2)
    }

def start_stream(schema_path = SCHEMA_PATH, interval=1):
    """Main Kafka streaming loop"""
    avro_schema = load_avro_schema(schema_path)
    if not avro_schema:
        return

    try:
        schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

        avro_serializer = AvroSerializer(
            schema_registry_client,
            json.dumps(avro_schema)
        )

        producer_config = {
            "bootstrap.servers": "kafka:9092",,
            "value.serializer": avro_serializer
        }

        producer = SerializingProducer(producer_config)

        logging.info("Starting Kafka Avro Producer...")

        while True:
            try:
                event = generate_event()
                producer.produce(
                    topic=TOPIC_NAME,
                    value=event
                )
                producer.poll(0)
                time.sleep(interval)
            except Exception as e:
                logging.error(f"Error producing message: {e}")

    except KeyboardInterrupt:
        logging.info("Producer stopped manually.")
    except Exception as e:
        logging.error(f"Producer failed: {e}")
    finally:
        producer.flush()

if __name__ == "__main__":
    start_stream()
