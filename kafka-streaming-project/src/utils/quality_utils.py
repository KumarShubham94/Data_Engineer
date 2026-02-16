"""
quality_utils.py

This module contains helper functions for performing data quality checks
on Delta tables or DataFrames. Designed for the Silver layer where we
enforce stricter expectations on cleaned data.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct

def check_not_null(df: DataFrame, columns: list) -> dict:
    """
    Check if specified columns contain null values.
    
    Args:
        df (DataFrame): Input Spark DataFrame.
        columns (list): List of column names to check.
    
    Returns:
        dict: Mapping of column -> number of null values.
    """
    null_counts = {}
    for col_name in columns:
        null_count = df.filter(col(col_name).isNull()).count()
        null_counts[col_name] = null_count
    return null_counts


def check_unique(df: DataFrame, column: str) -> bool:
    """
    Check if a column has unique values.
    
    Args:
        df (DataFrame): Input Spark DataFrame.
        column (str): Column name to check uniqueness.
    
    Returns:
        bool: True if all values are unique, False otherwise.
    """
    total_count = df.count()
    distinct_count = df.select(countDistinct(col(column))).collect()[0][0]
    return total_count == distinct_count


def check_column_types(df: DataFrame, expected_schema: dict) -> dict:
    """
    Validate column data types against expected schema.
    
    Args:
        df (DataFrame): Input Spark DataFrame.
        expected_schema (dict): Column name -> expected Spark type as string.
    
    Returns:
        dict: Columns that do not match the expected type.
    """
    mismatches = {}
    for col_name, expected_type in expected_schema.items():
        actual_type = dict(df.dtypes).get(col_name)
        if actual_type != expected_type:
            mismatches[col_name] = {"expected": expected_type, "actual": actual_type}
    return mismatches


def summarize_quality(df: DataFrame, not_null_columns: list, unique_columns: list, expected_schema: dict) -> None:
    """
    Perform all quality checks and print summary.
    
    Args:
        df (DataFrame): Input Spark DataFrame.
        not_null_columns (list): Columns to check for nulls.
        unique_columns (list): Columns to check uniqueness.
        expected_schema (dict): Expected column types.
    """
    print("----- Data Quality Summary -----")

    # Null checks
    null_summary = check_not_null(df, not_null_columns)
    print("Null Values per Column:")
    for col_name, null_count in null_summary.items():
        print(f"  {col_name}: {null_count}")

    # Uniqueness checks
    print("\nUniqueness Checks:")
    for col_name in unique_columns:
        is_unique = check_unique(df, col_name)
        print(f"  {col_name}: {'Unique' if is_unique else 'Duplicates'}")

    # Schema checks
    mismatched_types = check_column_types(df, expected_schema)
    print("\nSchema Type Mismatches:")
    if mismatched_types:
        for col_name, info in mismatched_types.items():
            print(f"  {col_name}: expected={info['expected']}, actual={info['actual']}")
    else:
        print("  All column types match")
    
    print("----- End of Summary -----")
