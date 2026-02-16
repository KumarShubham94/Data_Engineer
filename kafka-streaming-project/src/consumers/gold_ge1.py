import logging
import threading
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, from_unixtime
import great_expectations as ge

from src.utils.monitoring_utils import start_metrics_server, pipeline_timer

# ------------------------
# Config
# ------------------------
SILVER_PATH = "/Users/shubhamkumarverma/Documents/kafka-streaming-project/data/silver/events"
GOLD_PATH = "/Users/shubhamkumarverma/Documents/kafka-streaming-project/data/gold/gold_ge.parquet"

KEY_COLUMNS = ["event_id", "user_id", "campaign_id"]
TIMESTAMP_COLUMN = "timestamp"
METRICS_PORT = 8000

# Prevent accidental overwrite of Gold data
RECOMPUTE_GOLD = False

# ------------------------
# Logger setup
# ------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gold_ge")

# ------------------------
# Spark session
# ------------------------
def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Gold Aggregation with Great Expectations")
        .getOrCreate()
    )
    logger.info("Spark session ready")
    return spark

# ------------------------
# IO functions
# ------------------------
def read_silver_data(spark):
    df = spark.read.parquet(SILVER_PATH)
    logger.info(f"Read Silver data from {SILVER_PATH}")
    return df

def save_gold(df):
    if not RECOMPUTE_GOLD:
        logger.info("Skipping Gold write (already exists)")
        return

    df.write.mode("overwrite").parquet(GOLD_PATH)
    logger.info(f"Gold parquet saved at {GOLD_PATH}")

# ------------------------
# Transformations
# ------------------------
def convert_timestamp(df):
    """
    Convert epoch milliseconds (LongType) to TimestampType
    """
    return (
        df.withColumn(
            "timestamp_ts",
            from_unixtime(col(TIMESTAMP_COLUMN) / 1000).cast("timestamp")
        )
        .drop(TIMESTAMP_COLUMN)
        .withColumnRenamed("timestamp_ts", TIMESTAMP_COLUMN)
    )

def aggregate_gold(df):
    """
    Gold aggregation: daily events per campaign
    """
    return (
        df.withColumn("event_date", to_date(col(TIMESTAMP_COLUMN)))
          .groupBy("campaign_id", "event_date")
          .agg(count("*").alias("daily_event_count"))
    )

# ------------------------
# Data Quality with Great Expectations
# ------------------------
def run_data_quality_checks(df):
    """
    Enforces Gold-layer contract using Great Expectations
    """
    # Drop rows with null key columns
    df_clean = df.dropna(subset=KEY_COLUMNS)

    # Wrap Spark DF in GE
    ge_df = ge.dataset.SparkDFDataset(df_clean)

    # Key column checks
    for column in KEY_COLUMNS:
        ge_df.expect_column_values_to_not_be_null(column)
        ge_df.expect_column_values_to_be_of_type(column, "StringType")

    # Timestamp type check
    ge_df.expect_column_values_to_be_of_type(TIMESTAMP_COLUMN, "TimestampType")

    # Validate expectations
    results = ge_df.validate()
    if not results["success"]:
        logger.error("Data quality checks failed")
        logger.error(results)
        raise RuntimeError("Data quality validation failed")

    logger.info("Data quality checks passed")
    return df_clean

# ------------------------
# Pipeline
# ------------------------
def run_gold_aggregation():
    spark = create_spark_session()
    try:
        df_silver = read_silver_data(spark)
        df_ts = convert_timestamp(df_silver)
        df_clean = run_data_quality_checks(df_ts)
        df_gold = aggregate_gold(df_clean)
        save_gold(df_gold)
    finally:
        spark.stop()
        logger.info("Spark session stopped")

# ------------------------
# Entry point
# ------------------------
if __name__ == "__main__":
    logger.info("Starting Gold aggregation")

    # Start Prometheus metrics server in a background thread
    threading.Thread(target=start_metrics_server, args=(METRICS_PORT,), daemon=True).start()

    # Measure full pipeline runtime
    with pipeline_timer():
        run_gold_aggregation()

    # Keep the script alive so Prometheus can scrape metrics
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Metrics server stopped")
