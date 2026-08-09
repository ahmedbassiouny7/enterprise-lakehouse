"""Bronze write path with audit metadata."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit, to_date


def add_audit_columns(df: DataFrame, source: str) -> DataFrame:
    return df.withColumn("_bronze_ingested_at", current_timestamp()).withColumn(
        "_bronze_source", lit(source)
    )


def write_bronze_table(df: DataFrame, table_name: str, catalog: str = "lakehouse") -> None:
    """Full-overwrite write to lakehouse.bronze.<table_name>.

    Use this only for a source with no usable watermark column where a
    full re-read each run is genuinely fine — today that's just
    products (2,000 rows, no updated_at, cheap to fully re-read). Every
    other Bronze job has moved to append_bronze_table below (see
    docs/design-decisions.md, "Incremental extraction and its limits").

    Deliberately NOT partitioned by `_bronze_ingested_at` day: with
    createOrReplace() the table only ever holds one run's data, so a
    day-partition can never have more than one live value — partitioning
    on it would add Iceberg partition-management overhead for zero
    pruning benefit. That trade-off flips for append_bronze_table below,
    which is why that one IS partitioned by day.
    """
    (df.writeTo(f"{catalog}.bronze.{table_name}").createOrReplace())


def append_bronze_table(df: DataFrame, table_name: str, catalog: str = "lakehouse") -> None:
    """Incremental append write to lakehouse.bronze.<table_name>.

    Callers (bronze/extract_orders.py, extract_customers.py,
    extract_fx_rates.py) already filter df down to rows past the last
    watermark (see common/incremental.py) before calling this, so each
    run's rows are additive history, not a replacement snapshot — unlike
    write_bronze_table above. Partitioned by ingestion day: this is
    exactly the partitioning write_bronze_table's docstring says isn't
    worth it for a full-overwrite table, and it earns its keep here
    because Bronze now actually accumulates multiple days of runs, so
    day-pruning has something to prune.

    Creates the table on first call (nothing to append to yet); appends
    on every call after that.
    """
    full_name = f"{catalog}.bronze.{table_name}"
    df = df.withColumn("_bronze_ingested_date", to_date("_bronze_ingested_at"))
    writer = df.writeTo(full_name).partitionedBy("_bronze_ingested_date")
    try:
        exists = df.sparkSession.catalog.tableExists(full_name)
    except Exception:
        # Fall back to "assume it doesn't exist yet" — createOrReplace()
        # is still correct (if unnecessarily destructive of prior runs)
        # in the rare case this check itself fails; append() against a
        # table that doesn't exist would just fail loudly instead, which
        # is a worse first-run experience than this fallback.
        exists = False
    if exists:
        writer.append()
    else:
        writer.createOrReplace()