"""Shared Silver write path with monthly partitioning."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, months


def write_silver_table(
    df: DataFrame, table_name: str, business_date_col: str, catalog: str = "lakehouse"
) -> None:
    # Sort by business date before writing.
    df = df.withColumn("_silver_processed_at", current_timestamp())
    full_name = f"{catalog}.silver.{table_name}"
    df.sparkSession.sql(f"DROP TABLE IF EXISTS {full_name}")
    (
        df.orderBy(business_date_col)
        .writeTo(full_name)
        .partitionedBy(months(business_date_col))
        .create()
    )


def write_quarantine_table(df: DataFrame, table_name: str, catalog: str = "lakehouse") -> None:
    """Write quarantine rows without partitioning."""
    df = df.withColumn("_silver_processed_at", current_timestamp())
    full_name = f"{catalog}.quarantine.{table_name}"
    df.sparkSession.sql(f"DROP TABLE IF EXISTS {full_name}")
    df.writeTo(full_name).create()