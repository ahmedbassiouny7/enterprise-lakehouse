"""
Silver transform: products.

No FK dependency on another Silver table, so this (along with customers
and exchange_rates) is safe to run first / in parallel — orders and
order_items depend on this one for referential checks.

Run:
    docker exec master spark-submit /opt/spark-jobs/silver/transform_products.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyspark.sql.functions as F  # noqa: E402

from common.dq import DQCheck, apply_dq_checks  # noqa: E402
from common.silver_writer import write_quarantine_table, write_silver_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402


def main():
    spark = get_spark_session("silver_transform_products")

    bronze = spark.table("lakehouse.bronze.products")

    # Dedupe: keep the most-recently-ingested row per product_id. Bronze is
    # a full overwrite each run so duplicates shouldn't occur in practice,
    # but Silver shouldn't silently assume that holds forever — dedupe
    # defensively rather than trust an upstream invariant.
    from pyspark.sql import Window

    w = Window.partitionBy("product_id").orderBy(F.col("_bronze_ingested_at").desc())
    deduped = (
        bronze.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    checks = [
        DQCheck("product_id_not_null", F.col("product_id").isNotNull()),
        DQCheck("product_name_not_null", F.col("product_name").isNotNull()),
        DQCheck("unit_cost_non_negative", F.col("unit_cost") >= 0),
        DQCheck("list_price_non_negative", F.col("list_price") >= 0),
        DQCheck("list_price_gte_unit_cost", F.col("list_price") >= F.col("unit_cost")),
    ]
    good, quarantined = apply_dq_checks(deduped, checks)

    good_count, bad_count = good.count(), quarantined.count()
    print(f"[transform_products] {good_count:,} rows passed, {bad_count:,} quarantined")

    # No natural business date on products; partition by a constant
    # "epoch" column via current ingestion run instead of forcing a
    # meaningless date field to exist. Simpler: reuse _bronze_ingested_at,
    # since this table is small and full-reload anyway — partitioning
    # buys nothing here beyond consistency with the write helper's shape.
    write_silver_table(good, "products", business_date_col="_bronze_ingested_at")
    if bad_count > 0:
        write_quarantine_table(quarantined, "products")

    spark.stop()


if __name__ == "__main__":
    main()
