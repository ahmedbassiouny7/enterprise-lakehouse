"""Shared dedup helper for Silver transforms reading accumulating Bronze
tables.

Bronze tables written via common/bronze_writer.append_bronze_table
accumulate one row per source-row-per-run: a re-run, a backfill, or (for
products, which stays full-refresh) simply reading the same key twice
across runs can all put more than one row under the same business key
into Bronze. Silver must always collapse to exactly one row per key
before applying DQ checks or joining — this is that collapse, factored
out once instead of copy-pasted per job.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def dedupe_latest(
    df: DataFrame,
    key_cols: list,
    order_col: str = "_bronze_ingested_at",
) -> DataFrame:
    """Keep one row per key_cols: the row with the max order_col value."""
    w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
