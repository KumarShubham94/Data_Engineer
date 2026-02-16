"""
gold_aggregate.py

Gold-layer aggregation script.

Features implemented:
- Aggregation from Silver layer (campaign-level metrics)
- Idempotent writes / upsert into Delta table
- Partitioned by event_date for performance
- Basic audit columns (ingest_ts)
- First-run handling: creates Delta if not exists
- Incremental safe design (can be rerun)
"""

import os
import sys
import logging
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# -------------------------------------------------------------------
# Ensure src is on PYTHONPATH
# -------------------------------------------------------------------
SRC_ROOT = os.path.dirname(os.path.dirname(__file__))  # -> kafka-streaming-project/src
if SRC_ROOT not in sys.path:
    sys.path.insert(0, SRC_ROOT)

# -------------------------------------------------------------------
# Utils
# -------------------------------------------------------------------
from utils.delta_utils import create_spark_session, merge_delta, write_delta_table
from delta.tables import DeltaTable

# -------------------------------------------------------------------
# Configs / paths
# -------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
SILVER_PATH = os.path.join(BASE_DIR, "data/silver/events")
GOLD_PATH = os.path.join(BASE_DIR, "data/gold/aggregates")

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("gold-aggregate")

# -------------------------------------------------------------------
# Main aggregation
# -------------------------------------------------------------------
def run_gold_aggregation():
    logger.info("Starting Gold aggregation")

    spark = create_spark_session(app_name="GoldAggregate")
    logger.info("Spark session ready")

    # 1) Read Silver data
    try:
        df_silver = spark.read.format("delta").load(SILVER_PATH)
        logger.info("Read Silver data from %s", SILVER_PATH)
    except Exception as e:
        logger.exception("Failed to read Silver layer: %s", e)
        raise

    if df_silver.rdd.isEmpty():
        logger.info("No records in Silver to aggregate. Exiting.")
        return

    # 2) Aggregate metrics at campaign + date level
    df_agg = (
        df_silver.groupBy("campaign_id", "event_date")
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "open", 1).otherwise(0)).alias("opens"),
            F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("clicks"),
            F.avg("engagement_score").alias("avg_engagement_score")
        )
        .withColumn("ingest_ts", F.current_timestamp())
    )

    # 3) First-run / merge logic
    os.makedirs(GOLD_PATH, exist_ok=True)

    try:
        if not DeltaTable.isDeltaTable(spark, GOLD_PATH):
            # First run: write as Delta
            write_delta_table(df_agg, GOLD_PATH, mode="overwrite")
            logger.info("Gold Delta table created at first run")
        else:
            # Subsequent runs: merge/upsert
            merge_delta(
                df=df_agg,
                target_path=GOLD_PATH,
                join_columns=["campaign_id", "event_date"],
                update_columns=["total_events", "opens", "clicks", "avg_engagement_score", "ingest_ts"]
            )
            logger.info("Merge into Gold completed")
    except Exception as e:
        logger.exception("Gold aggregation failed: %s", e)
        raise

    logger.info("Gold aggregation finished successfully")


# -------------------------------------------------------------------
# Main guard
# -------------------------------------------------------------------
if __name__ == "__main__":
    try:
        run_gold_aggregation()
    except Exception as e:
        logger.exception("Gold aggregation failed: %s", e)
        raise
