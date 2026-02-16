"""
Delta Utilities

This module provides helper functions for reading and writing Delta tables.
It is meant to simplify Delta operations and centralize common patterns.
"""

import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# -------------------------------------------------------------------
# Spark Session Builder (Delta Enabled)
# -------------------------------------------------------------------
def create_spark_session(app_name="DeltaUtilsApp"):
    """
    Create a Spark session with Delta support.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# -------------------------------------------------------------------
# Delta Read / Write Utilities
# -------------------------------------------------------------------
def read_delta_table(spark, path):
    """
    Read a Delta table from the given path.
    
    Args:
        spark: SparkSession object
        path: str, path to the Delta table

    Returns:
        DataFrame
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Delta table path does not exist: {path}")
    return spark.read.format("delta").load(path)


def write_delta_table(df, path, mode="overwrite", checkpoint_path=None):
    """
    Write a DataFrame to Delta table.
    
    Args:
        df: DataFrame to write
        path: str, destination Delta table path
        mode: str, write mode ("overwrite", "append")
        checkpoint_path: str, path for streaming checkpoint (if streaming)
    """
    writer = df.write.format("delta").mode(mode)
    if checkpoint_path:
        writer = writer.option("checkpointLocation", checkpoint_path)
    writer.save(path)


def merge_delta(df, target_path, join_columns, update_columns):
    """
    Perform an upsert (merge) operation on a Delta table.

    If the target Delta table does not exist, it is created (initial load).
    On subsequent runs, data is merged incrementally.

    Args:
        df: DataFrame with new data
        target_path: str, path of target Delta table
        join_columns: list of str, columns used to match existing rows
        update_columns: list of str, columns to update if match is found
    """

    spark = df.sparkSession

    # ------------------------------------------------------------
    # FIRST RUN: create Delta table if it does not exist
    # ------------------------------------------------------------
    if not os.path.exists(target_path):
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(target_path)
        )
        return

    # ------------------------------------------------------------
    # SUBSEQUENT RUNS: MERGE (upsert)
    # ------------------------------------------------------------
    delta_table = DeltaTable.forPath(spark, target_path)

    merge_condition = " AND ".join(
        [f"target.{c} = source.{c}" for c in join_columns]
    )

    update_dict = {col: f"source.{col}" for col in update_columns}

    (
        delta_table.alias("target")
        .merge(
            df.alias("source"),
            merge_condition
        )
        .whenMatchedUpdate(set=update_dict)
        .whenNotMatchedInsertAll()
        .execute()
    )