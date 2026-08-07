"""Bronze write path with audit metadata and partitioning."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, days, lit


def add_audit_columns(df: DataFrame, source: str) -> DataFrame:
    return df.withColumn("_bronze_ingested_at", current_timestamp()).withColumn(
        "_bronze_source", lit(source)
    )


def write_bronze_table(df: DataFrame, table_name: str, catalog: str = "lakehouse") -> None:
    """Full-overwrite write to lakehouse.bronze.<table_name>, partitioned by ingestion day."""
    (
        df.writeTo(f"{catalog}.bronze.{table_name}")
        .partitionedBy(days("_bronze_ingested_at"))
        .createOrReplace()
    )
