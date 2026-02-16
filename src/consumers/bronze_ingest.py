"""
Bronze Ingestion Script

This module reads raw events from Kafka and writes them into the Bronze Delta table.
The Bronze layer stores data exactly as it arrives, with no transformations applied.
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from utils.kafka_utils import KAFKA_BOOTSTRAP, KAFKA_TOPIC
from utils.schema_utils import load_spark_schema

# -------------------------------------------------------------------
# Logging Setup
# -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("bronze-ingest")

# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

BRONZE_OUTPUT_PATH = os.path.join(BASE_DIR, "data/bronze/events")
BRONZE_CHECKPOINT_PATH = os.path.join(BASE_DIR, "checkpoints/bronze/events")

SCHEMA_PATH = os.path.join(BASE_DIR, "schemas/input_event.avsc")

# -------------------------------------------------------------------
# Spark Session Builder
# -------------------------------------------------------------------
def create_spark_session():
    """
    Create a Spark session configured to support Delta Lake.
    """
    spark = (
        SparkSession.builder.appName("BronzeIngestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# -------------------------------------------------------------------
# Bronze Ingestion Logic
# -------------------------------------------------------------------
def start_bronze_ingestion():
    """
    Read Kafka messages from the raw topic and write them directly to Delta Lake.
    """
    spark = create_spark_session()
    logger.info("Spark session created.")

    # Load Spark schema from Avro
    schema = load_spark_schema(SCHEMA_PATH)

    logger.info("Loading streaming data from Kafka topic '%s'.", KAFKA_TOPIC)

    # Read raw Kafka stream
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Kafka sends value as bytes → cast to string
    parsed_stream = (
        kafka_stream
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")  # rename Kafka timestamp
        .withColumn("json_data", from_json(col("json_str"), schema))
        .select("json_data.*", col("kafka_ts"))  # avoid duplicate column names
        .withColumn("data_source", lit("kafka"))
    )

    logger.info("Writing streaming data to Bronze Delta table at %s", BRONZE_OUTPUT_PATH)

    # Write to Delta with checkpointing
    query = (
        parsed_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
        .start(BRONZE_OUTPUT_PATH)
    )

    logger.info("Bronze ingestion streaming query started. Waiting for termination...")
    query.awaitTermination()

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
if __name__ == "__main__":
    try:
        start_bronze_ingestion()
    except Exception as exc:
        logger.exception("Bronze ingestion failed: %s", exc)
