"""Bronze write path with audit metadata."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit


def add_audit_columns(df: DataFrame, source: str) -> DataFrame:
    return df.withColumn("_bronze_ingested_at", current_timestamp()).withColumn(
        "_bronze_source", lit(source)
    )


def write_bronze_table(df: DataFrame, table_name: str, catalog: str = "lakehouse") -> None:
    """Full-overwrite write to lakehouse.bronze.<table_name>.

    Every Bronze job re-reads its ENTIRE source table each run (no
    incremental/CDC filter — see bronze/extract_orders.py etc.), so a full
    replace here is the correct match: each run's Bronze table is a
    complete, self-consistent snapshot of the source as of that run, not
    an accumulating log.

    Deliberately NOT partitioned by `_bronze_ingested_at` day. That was
    tried first and reverted: with createOrReplace() the table only ever
    holds one run's data, so a day-partition can never have more than one
    live value — partitioning on it adds Iceberg partition-management
    overhead for zero pruning benefit. Partitioning by day only pays off
    once Bronze actually accumulates history across runs, which would
    require incremental extraction (a real, separate change — see
    docs/design-decisions.md) and, for orders/order_items specifically,
    dedup logic in Silver that doesn't exist there today. Don't add day
    partitioning back without that groundwork, or a second run will
    silently duplicate every order.
    """
    (df.writeTo(f"{catalog}.bronze.{table_name}").createOrReplace())