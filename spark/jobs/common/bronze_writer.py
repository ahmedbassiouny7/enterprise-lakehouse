"""
Shared Bronze-layer write path: audit/lineage columns + partitioning.

Partitioning decision (worth being able to defend in an interview):
Bronze tables are partitioned by INGESTION date (`days(_bronze_ingested_at)`),
not by a business-meaningful date like order_date. This is deliberate:

  - Bronze's job is to support reprocessing/backfill by *load batch*, not
    to be queried by business analysts — that's what Silver/Gold are for.
    Partitioning by ingestion date means "reprocess everything we loaded
    on 2026-08-05" is a cheap partition-pruned operation.
  - Not every Bronze source has an obvious business date column (e.g.
    products, customers don't). Ingestion-date partitioning is uniform
    across every Bronze table regardless of source shape, so this one
    helper works for all of them instead of a bespoke partition column
    per job.
  - Business-date partitioning (order_date, signup_date, etc.) is applied
    in Silver, where it actually matches how downstream consumers query.

Bronze load strategy is full overwrite (createOrReplace), not incremental
append. This is an explicit, known limitation: the generator's schema has
no updated_at/watermark column, so there's no reliable way to identify
"what changed since last run." At real scale this would need either a
watermark column on the sources or CDC (e.g. Debezium) feeding Bronze
incrementally. Flagging this in docs/design-decisions.md rather than
quietly doing a full reload and hoping nobody asks.
"""
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
