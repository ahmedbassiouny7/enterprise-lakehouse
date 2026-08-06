"""
Shared Silver-layer write path.

Partitioning: unlike Bronze (partitioned by ingestion date — see
bronze_writer.py), Silver partitions by a BUSINESS date column, because
Silver is where downstream consumers (Gold jobs, Trino ad-hoc queries)
actually filter and aggregate — "revenue for March 2025" should be a
partition-pruned scan, not a full-table scan with a filter. Partition
granularity is month, not day: 2 years of data at daily granularity is
~730 tiny partitions for what's still a modest row count (100K orders) —
more partition-management overhead than query benefit at this scale.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, months


def write_silver_table(
    df: DataFrame, table_name: str, business_date_col: str, catalog: str = "lakehouse"
) -> None:
    # Iceberg's writer (ClusteredDataWriter, used by createOrReplace/CTAS)
    # requires rows to arrive already grouped by partition value — it opens
    # a file per partition, closes it once the value changes, and errors if
    # that value reappears later ("Incoming records violate the writer
    # assumption that records are clustered..."). Source read order (JDBC
    # with no ORDER BY, upstream file order, etc.) is never guaranteed to
    # match business-date order, so an explicit sort here is required, not
    # optional — this bit real data (customers) on first live run despite
    # looking fine against products/exchange_rates, whose read order
    # happened to already be grouped by coincidence.
    df = df.withColumn("_silver_processed_at", current_timestamp())
    (
        df.orderBy(months(business_date_col))
        .writeTo(f"{catalog}.silver.{table_name}")
        .partitionedBy(months(business_date_col))
        .createOrReplace()
    )


def write_quarantine_table(df: DataFrame, table_name: str, catalog: str = "lakehouse") -> None:
    """No partitioning — quarantine tables are small and queried ad hoc
    ("show me everything DQ rejected today"), not scanned at Gold-job
    volume, so partition-pruning isn't worth the added complexity here."""
    df = df.withColumn("_silver_processed_at", current_timestamp())
    df.writeTo(f"{catalog}.quarantine.{table_name}").createOrReplace()