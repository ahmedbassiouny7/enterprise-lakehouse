"""Shared Silver write path with monthly partitioning."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, months


def write_silver_table(
    df: DataFrame, table_name: str, business_date_col: str, catalog: str = "lakehouse"
) -> None:
    # Sort by business date before writing.
    df = df.withColumn("_silver_processed_at", current_timestamp())
    (
        df.orderBy(business_date_col)
        .writeTo(f"{catalog}.silver.{table_name}")
        .partitionedBy(months(business_date_col))
        .createOrReplace()
    )


def write_quarantine_table(df: DataFrame, table_name: str, catalog: str = "lakehouse") -> None:
    """Write quarantine rows without partitioning."""
    df = df.withColumn("_silver_processed_at", current_timestamp())
    df.writeTo(f"{catalog}.quarantine.{table_name}").createOrReplace()