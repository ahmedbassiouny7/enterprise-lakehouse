"""Unit tests for spark/jobs/common/dedupe.py — dedupe_latest.

Exercised on hand-built DataFrames shaped like the accumulating Bronze
tables it runs against in production (see silver/transform_orders.py,
transform_customers.py, etc.) — not on live Iceberg tables.
"""
from datetime import datetime

from common.dedupe import dedupe_latest


def test_single_row_per_key_is_unaffected(spark):
    df = spark.createDataFrame(
        [(1, "a", datetime(2026, 1, 1))], ["id", "value", "_bronze_ingested_at"]
    )
    result = dedupe_latest(df, key_cols=["id"])

    assert result.count() == 1
    assert result.collect()[0]["value"] == "a"


def test_keeps_the_row_with_the_latest_ingested_at(spark):
    df = spark.createDataFrame(
        [
            (1, "old", datetime(2026, 1, 1)),
            (1, "new", datetime(2026, 1, 2)),
        ],
        ["id", "value", "_bronze_ingested_at"],
    )
    result = dedupe_latest(df, key_cols=["id"])
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["value"] == "new"


def test_dedupe_is_independent_per_key(spark):
    df = spark.createDataFrame(
        [
            (1, "a-old", datetime(2026, 1, 1)),
            (1, "a-new", datetime(2026, 1, 2)),
            (2, "b-only", datetime(2026, 1, 1)),
        ],
        ["id", "value", "_bronze_ingested_at"],
    )
    result = dedupe_latest(df, key_cols=["id"])
    values = {r["id"]: r["value"] for r in result.collect()}

    assert values == {1: "a-new", 2: "b-only"}


def test_composite_key_columns(spark):
    # Mirrors the real usage in silver/transform_exchange_rates.py: a
    # 3-column business key, not a single surrogate id.
    df = spark.createDataFrame(
        [
            ("2026-01-01", "USD", "EUR", 0.90, datetime(2026, 1, 1)),
            ("2026-01-01", "USD", "EUR", 0.92, datetime(2026, 1, 2)),
            ("2026-01-01", "USD", "GBP", 0.79, datetime(2026, 1, 1)),
        ],
        ["rate_date", "base_currency", "quote_currency", "rate", "_bronze_ingested_at"],
    )
    result = dedupe_latest(df, key_cols=["rate_date", "base_currency", "quote_currency"])
    rates = {r["quote_currency"]: r["rate"] for r in result.collect()}

    assert rates == {"EUR": 0.92, "GBP": 0.79}
