"""
Utility helpers for loading and managing Avro/JSON/Spark schemas.

This module keeps all schema-loading logic in one place so that
producers, consumers, and Spark pipelines stay consistent.
"""

import json
import logging
from confluent_kafka import avro
from pyspark.sql import SparkSession

logger = logging.getLogger("schema-utils")


# -------------------------------------------------------------------
# Avro schema loader (for Kafka producer & Schema Registry)
# -------------------------------------------------------------------
def load_avro_schema(path: str):
    """
    Load an Avro schema from a .avsc file (for Kafka producer/consumer).
    Returns an Avro schema object (confluent_kafka.avro).
    """
    try:
        with open(path, "r") as f:
            schema_str = f.read()

        schema = avro.loads(schema_str)
        logger.info("Loaded Avro schema from %s", path)
        return schema

    except Exception as exc:
        logger.error("Failed to load Avro schema from %s: %s", path, exc)
        raise


# -------------------------------------------------------------------
# JSON Schema loader (not required in Phase 2, but clean design)
# -------------------------------------------------------------------
def load_json_schema(path: str):
    """
    Load JSON schema from a file used in validation or transformations.
    Returns Python dictionary.
    """
    try:
        with open(path, "r") as f:
            schema = json.load(f)

        logger.info("Loaded JSON schema from %s", path)
        return schema

    except Exception as exc:
        logger.error("Failed to load JSON schema from %s: %s", path, exc)
        raise


# -------------------------------------------------------------------
# Spark StructType loader (Required for Bronze ingestion)
# -------------------------------------------------------------------
def load_spark_schema(path: str):
    """
    Load Avro schema (.avsc) and convert it to a Spark StructType.

    Spark does not understand Avro schema directly, so the trick is:
    1. Create a temporary Spark session
    2. Use spark.read.format("avro") with the schema file
    3. Extract the schema as StructType

    This avoids manually converting Avro → StructType.
    """

    try:
        spark = (
            SparkSession.builder
            .appName("SchemaLoaderTemp")
            .master("local[*]")
            .getOrCreate()
        )

        # Create an empty DF with Avro schema applied
        df = (
            spark.read.format("avro")
            .option("avroSchema", open(path).read())
            .load()  # creates an empty DF with schema
        )

        spark_schema = df.schema
        logger.info("Loaded Spark StructType schema from %s", path)
        return spark_schema

    except Exception as exc:
        logger.error("Failed to load Spark schema from %s: %s", path, exc)
        raise
