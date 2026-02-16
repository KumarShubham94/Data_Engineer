import time
import random
import logging
from faker import Faker
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from src.utils.schema_utils import load_avro_schema

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
SCHEMA_PATH = "schemas/input_event.avsc"
TOPIC = "email-marketing-events"

# IMPORTANT: host-side Kafka listener (see docker-compose.yml)
BOOTSTRAP_SERVERS = "localhost:29092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

# -------------------------------------------------------------------
# Logging Setup
# -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("faker-producer")

fake = Faker()

def create_fake_event() -> dict:
    """Return a single fake email-marketing event (dict) matching the Avro schema."""
    event_types = ["sent", "open", "click", "unsubscribe"]
    devices = ["mobile", "desktop", "tablet"]
    countries = ["US", "UK", "CA", "IN", "DE", "AU"]

    event_type = random.choice(event_types)
    return {
        "event_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "campaign_id": f"CMP_{random.randint(100, 999)}",
        "email_subject": fake.sentence(nb_words=4),
        "event_type": event_type,
        "timestamp": int(time.time() * 1000),
        "device_type": random.choice(devices),
        "country": random.choice(countries),
        "engagement_score": round(random.uniform(0, 1), 2),
        "email_open_time": random.randint(0, 120) if event_type in ("open", "click") else 0
    }


def create_producer(value_schema):
    """Create and return an AvroProducer configured to use local Schema Registry."""
    try:
        producer_config = {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "schema.registry.url": SCHEMA_REGISTRY_URL
        }
        producer = AvroProducer(producer_config, default_value_schema=value_schema)
        return producer
    except Exception as exc:
        logger.error("Error creating AvroProducer: %s", exc)
        raise


def produce_messages(producer, topic: str):
    """Continuously produce messages to Kafka (1 per second)."""
    logger.info("Starting to produce messages to topic '%s'.", topic)
    try:
        while True:
            event = create_fake_event()
            try:
                producer.produce(topic=topic, value=event)
                producer.flush()
                logger.info("Produced event: %s", event)
            except Exception as produce_err:
                logger.error("Error producing message: %s", produce_err)
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    except Exception as exc:
        logger.exception("Unexpected producer error: %s", exc)
    finally:
        logger.info("Shutting down producer.")


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def main():
    try:
        value_schema = load_avro_schema(SCHEMA_PATH)
        producer = create_producer(value_schema)
        produce_messages(producer, TOPIC)
    except Exception as exc:
        logger.exception("Producer initialization failed: %s", exc)


if __name__ == "__main__":
    main()
