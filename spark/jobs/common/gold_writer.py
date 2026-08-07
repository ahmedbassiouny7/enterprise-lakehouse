"""Shared Gold-layer write path without partitioning."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp


def write_gold_table(df: DataFrame, table_name: str, catalog: str = "lakehouse") -> None:
    df = df.withColumn("_gold_computed_at", current_timestamp())
    df.writeTo(f"{catalog}.gold.{table_name}").createOrReplace()
