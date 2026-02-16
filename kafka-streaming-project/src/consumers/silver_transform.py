"""
silver_transform.py

Silver-layer transformation script (batch / incremental).

Features implemented (simple, explainable):
 - Schema enforcement (compare to expectations)
 - Controlled evolution (merge into Delta with upsert)
 - Column standardization (snake_case)
 - Data quality validation (nulls, uniqueness, ranges) using quality_utils
 - Deduplication (keep latest by event timestamp)
 - Incremental processing via a small checkpoint (last_processed_ts)
 - Standardization & enrichment (derived columns)
 - Handling late-arriving data via merge/upsert
 - Checkpointing / basic reliability (writes are idempotent via MERGE)
 - Basic audit columns (ingest_ts, source)
 - Partitioning suggestion (by date for performance)
"""

import os
import sys

# -------------------------------------------------------------------
# Ensure src is on PYTHONPATH (MUST be before utils imports)
# -------------------------------------------------------------------
SRC_ROOT = os.path.dirname(os.path.dirname(__file__))  # -> kafka-streaming-project/src
if SRC_ROOT not in sys.path:
    sys.path.insert(0, SRC_ROOT)

import json
import logging
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession

# local utils (assumes you run from project root and PYTHONPATH includes src/)
from utils.schema_utils import load_json_schema
from utils.quality_utils import summarize_quality, check_not_null, check_unique, check_column_types
from utils.delta_utils import create_spark_session, merge_delta

# -------------------------------------------------------------------
# Configs / paths
# -------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

BRONZE_PATH = os.path.join(BASE_DIR, "data/bronze/events")
SILVER_PATH = "/Users/shubhamkumarverma/Documents/kafka-streaming-project/data/silver"
SILVER_TABLE_PATH = os.path.join(SILVER_PATH, "events")

EXPECTATIONS_PATH = os.path.join(BASE_DIR, "configs/expectations/silver_expectations.json")
CHECKPOINT_FILE = os.path.join(BASE_DIR, "checkpoints/silver/last_processed_ts.json")

# Business keys and columns
JOIN_COLUMNS = ["event_id"]  # primary key for dedup/upsert
# choose update columns (columns you want refreshed on merge)
UPDATE_COLUMNS = [
    "user_id",
    "campaign_id",
    "email_subject",
    "event_type",
    "timestamp",
    "device_type",
    "country",
    "engagement_score",
    "email_open_time",
    "kafka_ts",
    "data_source",
    "ingest_ts",
    "ingest_source"
]

# Partition column for Silver table (helps performance)
PARTITION_BY = "event_date"

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("silver-transform")


# -------------------------------------------------------------------
# Helpers: checkpoint simple read/write (keeps last processed epoch ms)
# -------------------------------------------------------------------
def read_last_checkpoint():
    """Read last processed timestamp from checkpoint file (epoch ms)."""
    try:
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, "r") as f:
                payload = json.load(f)
            return int(payload.get("last_processed_ts", 0))
    except Exception:
        logger.warning("Failed to read checkpoint file, will process all data.")
    return 0


def write_last_checkpoint(ts_ms: int):
    """Write last processed timestamp to checkpoint file."""
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_processed_ts": int(ts_ms)}, f)


# -------------------------------------------------------------------
# Main transform
# -------------------------------------------------------------------
def run_silver_transform():
    logger.info("Starting Silver transform")

    # 1) Load expectations (contains schema and rules)
    expectations = load_json_schema(EXPECTATIONS_PATH)
    # build smaller helpers from expectations
    expected_columns = [e["column"] for e in expectations.get("expectations", [])]
    not_null_columns = [e["column"] for e in expectations.get("expectations", []) if e.get("expectation_type") == "not_null"]
    unique_columns = [e["column"] for e in expectations.get("expectations", []) if e.get("expectation_type") == "unique"]

    # expected schema type map (for check_column_types)
    expected_schema_map = {}
    for item in expectations.get("expectations", []):
        if "data_type" in item:
            # map to strings used by df.dtypes: "string", "int", "bigint", "double", "float", "timestamp"
            expected_schema_map[item["column"]] = item["data_type"]

    # Deduplication config (from expectations)
    dedupe_cfg = expectations.get("deduplication", {})
    primary_key = dedupe_cfg.get("primary_key", JOIN_COLUMNS)

    # 2) Spark session with Delta
    spark = create_spark_session(app_name="SilverTransform")
    logger.info("Spark session ready")

    # 3) Read Bronze data (Delta or Parquet)
    try:
        bronze_df = spark.read.format("delta").load(BRONZE_PATH)
        logger.info("Read Bronze as Delta from %s", BRONZE_PATH)
    except Exception:
        bronze_df = spark.read.parquet(BRONZE_PATH)
        logger.info("Read Bronze as Parquet from %s", BRONZE_PATH)

    # 4) Incremental filtering: only process records newer than last checkpoint
    last_ts = read_last_checkpoint()
    logger.info("Last checkpoint (ms): %s", last_ts)
    if last_ts > 0:
        df_new = bronze_df.filter(F.col("timestamp") > F.lit(last_ts))
    else:
        df_new = bronze_df

    if df_new.rdd.isEmpty():
        logger.info("No new records to process. Exiting.")
        return

    # 5) Column naming standardization: convert to snake_case if needed
    #      (Here columns are already in snake_case; snippet kept to show approach)
    for c in df_new.columns:
        safe_name = c.strip()
        if safe_name != c:
            df_new = df_new.withColumnRenamed(c, safe_name)

    # 6) Basic enrichment / standardization
    df_enriched = (
        df_new
        .withColumn("ingest_ts", F.current_timestamp())  # ingestion time
        .withColumn("ingest_source", F.lit("bronze"))
        # event_date for partitioning (UTC date derived from timestamp milliseconds)
        .withColumn("event_date", F.to_date(F.from_unixtime(F.col("timestamp") / 1000).cast("timestamp")))
        # normalize engagement_score to float if not already
        .withColumn("engagement_score", F.col("engagement_score").cast("double"))
    )

    # 7) Schema enforcement: check that required columns exist and types are as expected
    missing_cols = [c for c in expected_columns if c not in df_enriched.columns]
    if missing_cols:
        logger.error("Schema enforcement failed. Missing required columns: %s", missing_cols)
        raise RuntimeError(f"Missing required Bronze columns: {missing_cols}")

    type_mismatches = check_column_types(df_enriched, expected_schema_map)
    if type_mismatches:
        logger.warning("Type mismatches detected: %s", type_mismatches)
        # For silver layer we fail fast; in production we might route to DLQ
        raise RuntimeError(f"Type mismatches: {type_mismatches}")

    # 8) Data quality checks (summary). These functions are simple and fast for moderate-sized sets.
    logger.info("Running quality checks")
    summarize_quality(df_enriched, not_null_columns, unique_columns, expected_schema_map)

    # 9) Deduplication: keep latest record per primary key using window function
    #    This ensures deterministic deduplication before merge.
    w = Window.partitionBy(*primary_key).orderBy(F.col("timestamp").desc(), F.col("ingest_ts").desc())
    df_ranked = df_enriched.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # 10) Prepare for merge: select only columns we intend to persist (update or insert)
    final_cols = [c for c in df_ranked.columns if c in UPDATE_COLUMNS or c in JOIN_COLUMNS or c in (PARTITION_BY, "ingest_ts", "ingest_source")]
    # guarantee join_columns included
    for k in JOIN_COLUMNS:
        if k not in final_cols:
            final_cols.append(k)
    df_to_merge = df_ranked.select(*final_cols)

    # 11) MERGE into Silver (idempotent, incremental)
    os.makedirs(SILVER_PATH, exist_ok=True)
    try:
        merge_delta(
            df=df_to_merge,
            target_path=SILVER_TABLE_PATH,
            join_columns=primary_key,
            update_columns=[c for c in final_cols if c not in primary_key]
        )
        logger.info("Merge into Silver completed")
    except Exception as exc:
        logger.exception("Merge into Silver failed: %s", exc)
        raise

    # 12) Update checkpoint to latest processed timestamp (max timestamp in this batch)
    max_ts_row = df_ranked.select(F.max(F.col("timestamp")).alias("max_ts")).collect()
    if max_ts_row and len(max_ts_row) > 0:
        max_ts = max_ts_row[0]["max_ts"] or last_ts
        write_last_checkpoint(int(max_ts))
        logger.info("Checkpoint updated to %s", max_ts)

    logger.info("Silver transform finished successfully")


# -------------------------------------------------------------------
# Main guard
# -------------------------------------------------------------------
if __name__ == "__main__":
    try:
        run_silver_transform()
    except Exception as e:
        logger.exception("Silver transform failed: %s", e)
        raise
