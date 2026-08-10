"""Unit tests for spark/jobs/common/incremental.py -- get_watermark/set_watermark.

Runs against a local Iceberg catalog (HadoopCatalog, no Hive Metastore --
see the iceberg_spark/clean_watermarks fixtures in conftest.py), not the
real lakehouse. Automatically skipped if the Iceberg runtime jar hasn't
been downloaded yet (see docker/spark-jars/README.md) -- these are the
only tests in this suite with that prerequisite.
"""
from common.incremental import get_watermark, set_watermark


def test_get_watermark_is_none_before_any_write(clean_watermarks):
    assert get_watermark(clean_watermarks, "orders") is None


def test_set_then_get_roundtrip(clean_watermarks):
    set_watermark(clean_watermarks, "orders", "2026-01-15 00:00:00")

    assert get_watermark(clean_watermarks, "orders") == "2026-01-15 00:00:00"


def test_set_watermark_upserts_rather_than_duplicates(clean_watermarks):
    # Mirrors real usage: extract_orders.py calls set_watermark once per
    # successful run, for the same source_table each time. A naive INSERT
    # instead of MERGE would leave one row per run instead of one row
    # per source_table, and get_watermark's LIMIT-less SELECT would then
    # return however many rows collect() happens to order first.
    set_watermark(clean_watermarks, "orders", "2026-01-15 00:00:00")
    set_watermark(clean_watermarks, "orders", "2026-01-20 00:00:00")

    rows = clean_watermarks.sql(
        "SELECT watermark_value FROM lakehouse.control.watermarks "
        "WHERE source_table = 'orders'"
    ).collect()

    assert len(rows) == 1
    assert rows[0]["watermark_value"] == "2026-01-20 00:00:00"
    assert get_watermark(clean_watermarks, "orders") == "2026-01-20 00:00:00"


def test_watermarks_are_independent_per_source_table(clean_watermarks):
    set_watermark(clean_watermarks, "orders", "2026-01-15 00:00:00")
    set_watermark(clean_watermarks, "customers", "500")

    assert get_watermark(clean_watermarks, "orders") == "2026-01-15 00:00:00"
    assert get_watermark(clean_watermarks, "customers") == "500"
    # A table that was never set stays unset, not "inherits" another's value.
    assert get_watermark(clean_watermarks, "exchange_rates") is None
