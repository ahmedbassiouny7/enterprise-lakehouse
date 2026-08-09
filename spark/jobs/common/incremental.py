"""Watermark-based incremental extraction for Bronze jobs.

Scope, stated explicitly: this catches NEW rows past a watermark column
(a timestamp, a date, or a monotonically increasing surrogate key). It
does NOT catch UPDATES to a row already extracted in an earlier run —
e.g. an order's status flipping from PLACED to SHIPPED after ingestion,
or a customer's is_active flag changing.

That's not an oversight; none of this project's source tables expose an
updated_at/modified_at column, so there is nothing to compare against to
detect an in-place edit short of re-reading the entire source table every
run (which is exactly the full-refresh approach this change moves away
from). Closing that gap for real requires either adding an updated_at
column to the source schema (with an app-level guarantee that every write
sets it) or CDC (e.g. Debezium reading the WAL/binlog). Both are source-
system changes, not something a Bronze job can retrofit on its own. See
docs/design-decisions.md, "Incremental extraction and its limits."
"""
from typing import Optional

from pyspark.sql import SparkSession

WATERMARK_TABLE = "lakehouse.control.watermarks"


def _ensure_watermark_table(spark: SparkSession) -> None:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} "
        "(source_table STRING, watermark_value STRING, updated_at TIMESTAMP) "
        "USING iceberg"
    )


def get_watermark(spark: SparkSession, source_table: str) -> Optional[str]:
    """Return the stored watermark for source_table, or None on first run
    (no row yet) — callers treat None as "do a full extract this once"."""
    _ensure_watermark_table(spark)
    rows = spark.sql(
        f"SELECT watermark_value FROM {WATERMARK_TABLE} "
        f"WHERE source_table = '{source_table}'"
    ).collect()
    return rows[0]["watermark_value"] if rows else None


def set_watermark(spark: SparkSession, source_table: str, value: str) -> None:
    """Upsert the new watermark for source_table after a successful,
    non-empty extraction. Callers must NOT call this when zero new rows
    were read — an unchanged watermark is the correct outcome then, not a
    write of the same value."""
    _ensure_watermark_table(spark)
    spark.sql(
        f"""
        MERGE INTO {WATERMARK_TABLE} t
        USING (SELECT '{source_table}' AS source_table,
                      '{value}' AS watermark_value) s
        ON t.source_table = s.source_table
        WHEN MATCHED THEN UPDATE SET
            t.watermark_value = s.watermark_value,
            t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (source_table, watermark_value, updated_at)
            VALUES (s.source_table, s.watermark_value, current_timestamp())
        """
    )
