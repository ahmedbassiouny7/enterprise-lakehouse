"""
Shared Gold-layer write path.

No partitioning — Gold tables here are aggregates (thousands of rows at
most, e.g. one row per product or per month), not the ~100K-row fact
tables Bronze/Silver hold. Partitioning a table that fits in one Trino
split adds bookkeeping with no pruning benefit; that's a Bronze/Silver
concern, not a Gold one.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp


def write_gold_table(df: DataFrame, table_name: str, catalog: str = "lakehouse") -> None:
    df = df.withColumn("_gold_computed_at", current_timestamp())
    df.writeTo(f"{catalog}.gold.{table_name}").createOrReplace()
