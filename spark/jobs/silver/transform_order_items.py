"""Silver transform for order_items."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyspark.sql.functions as F  # noqa: E402

from common.dedupe import dedupe_latest  # noqa: E402
from common.dq import DQCheck, apply_dq_checks  # noqa: E402
from common.silver_writer import write_quarantine_table, write_silver_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402


def main():
    spark = get_spark_session("silver_transform_order_items")

    # Load-bearing: bronze.order_items is append-only across runs (see
    # bronze/extract_orders.py), so a re-run or backfill overlapping an
    # already-extracted order_item_id watermark can put the same
    # order_item_id in Bronze twice.
    bronze_items = dedupe_latest(
        spark.table("lakehouse.bronze.order_items"), key_cols=["order_item_id"]
    )
    silver_orders = spark.table("lakehouse.silver.orders").select(
        F.col("order_id").alias("_known_order_id"),
        F.col("order_date"),
        F.col("fx_rate_to_usd"),
    )
    silver_products = spark.table("lakehouse.silver.products").select(
        F.col("product_id").alias("_known_product_id")
    )

    joined = (
        bronze_items.join(
            silver_orders, bronze_items.order_id == silver_orders._known_order_id, how="left"
        ).join(
            silver_products,
            bronze_items.product_id == silver_products._known_product_id,
            how="left",
        )
    )

    transformed = joined.withColumn(
        "line_total_usd", F.round(F.col("line_total") / F.col("fx_rate_to_usd"), 2)
    )

    checks = [
        DQCheck("order_item_id_not_null", F.col("order_item_id").isNotNull()),
        DQCheck("order_id_exists", F.col("_known_order_id").isNotNull()),
        DQCheck("product_id_exists", F.col("_known_product_id").isNotNull()),
        DQCheck("quantity_positive", F.col("quantity") > 0),
        DQCheck("unit_price_non_negative", F.col("unit_price") >= 0),
    ]
    good, quarantined = apply_dq_checks(transformed, checks)
    drop_cols = ["_known_order_id", "_known_product_id", "fx_rate_to_usd"]
    good = good.drop(*drop_cols)
    quarantined = quarantined.drop(*drop_cols)

    good_count, bad_count = good.count(), quarantined.count()
    print(f"[transform_order_items] {good_count:,} rows passed, {bad_count:,} quarantined")

    write_silver_table(good, "order_items", business_date_col="order_date")
    if bad_count > 0:
        write_quarantine_table(quarantined, "order_items")

    spark.stop()


if __name__ == "__main__":
    main()
