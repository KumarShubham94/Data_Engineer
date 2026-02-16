import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, from_unixtime
import great_expectations as ge

# ------------------------
# Config
# ------------------------
SILVER_PATH = "/Users/shubhamkumarverma/Documents/kafka-streaming-project/data/silver/events"
GOLD_PATH = "/Users/shubhamkumarverma/Documents/kafka-streaming-project/data/gold/gold_ge.parquet"
KEY_COLUMNS = ["event_id", "user_id", "campaign_id"]
TIMESTAMP_COLUMN = "timestamp"

# ------------------------
# Logger setup
# ------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gold_ge")

# ------------------------
# Functions
# ------------------------
def create_spark_session():
    spark = SparkSession.builder.appName("Gold Aggregation with GE").getOrCreate()
    logger.info("Spark session ready")
    return spark

def read_silver_data(spark):
    try:
        df = spark.read.parquet(SILVER_PATH)
        logger.info(f"Read Silver data from {SILVER_PATH}")
        return df
    except Exception as e:
        logger.error(f"Failed to read Silver layer: {e}")
        spark.stop()
        raise

def convert_timestamp(df):
    # Convert LongType timestamp (epoch ms) to proper TimestampType
    df_converted = df.withColumn("timestamp_ts", from_unixtime(col(TIMESTAMP_COLUMN)/1000).cast("timestamp"))
    return df_converted.drop(TIMESTAMP_COLUMN).withColumnRenamed("timestamp_ts", TIMESTAMP_COLUMN)

def run_data_quality_checks(df):
    # Drop nulls in key columns automatically
    df_clean = df.dropna(subset=KEY_COLUMNS)

    # Wrap in Great Expectations
    ge_df = ge.dataset.SparkDFDataset(df_clean)

    # Null checks
    for col_name in KEY_COLUMNS:
        ge_df.expect_column_values_to_not_be_null(col_name)

    # Schema / type checks
    ge_df.expect_column_values_to_be_of_type("event_id", "StringType")
    ge_df.expect_column_values_to_be_of_type("user_id", "StringType")
    ge_df.expect_column_values_to_be_of_type("campaign_id", "StringType")
    ge_df.expect_column_values_to_be_of_type(TIMESTAMP_COLUMN, "TimestampType")

    results = ge_df.validate()
    if not results["success"]:
        logger.error(f"Data quality checks failed: {results}")
        raise RuntimeError("Data quality validation failed.")

    logger.info("Data quality checks passed")
    return df_clean

def aggregate_gold(df):
    df_gold = df.withColumn("event_date", to_date(col(TIMESTAMP_COLUMN))) \
                .groupBy("campaign_id", "event_date") \
                .agg(count("*").alias("daily_event_count"))
    return df_gold

def save_gold(df):
    try:
        df.write.mode("overwrite").parquet(GOLD_PATH)
        logger.info(f"Gold parquet saved at {GOLD_PATH}")
    except Exception as e:
        logger.error(f"Failed to save Gold parquet: {e}")
        raise

def run_gold_aggregation():
    spark = create_spark_session()
    try:
        df_silver = read_silver_data(spark)
        df_silver_ts = convert_timestamp(df_silver)  # Convert timestamp to proper type
        df_clean = run_data_quality_checks(df_silver_ts)
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
    run_gold_aggregation()
