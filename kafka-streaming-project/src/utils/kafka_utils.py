import logging
from confluent_kafka.avro import AvroConsumer
from confluent_kafka import Consumer

# kafka_utils.py

KAFKA_BOOTSTRAP = "localhost:29092"
KAFKA_TOPIC = "email-marketing-events"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka-utils")


def create_kafka_consumer(
    topic_name: str,
    bootstrap_servers: str = "localhost:29092",
    schema_registry_url: str = "http://localhost:8081",
    group_id: str = "bronze-layer-consumer",
):
    """
    Create and return a configured AvroConsumer.

    The topic_name should match the producer topic
    (e.g., 'email-marketing-events').
    """

    consumer_config = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "schema.registry.url": schema_registry_url,
    }

    logger.info("Creating Kafka AvroConsumer...")
    consumer = AvroConsumer(consumer_config)
    consumer.subscribe([topic_name])
    logger.info(f"Subscribed to Kafka topic '{topic_name}'")

    return consumer


def poll_message(consumer, timeout: float = 1.0):
    """
    Poll for a single Kafka message and decode it.
    """
    msg = consumer.poll(timeout)

    if msg is None:
        return None

    if msg.error():
        logger.error(f"Kafka message error: {msg.error()}")
        return None

    try:
        return msg.value()  # Avro => dict
    except Exception as exc:
        logger.error(f"Error deserializing Avro message: {exc}")
        return None


def close_consumer(consumer):
    """
    Cleanly close the Kafka consumer.
    """
    try:
        consumer.close()
        logger.info("Kafka consumer closed successfully.")
    except Exception as exc:
        logger.error(f"Error closing consumer: {exc}")
