"""Silver transform for orders with referential and FX validation."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyspark.sql.functions as F  # noqa: E402

from common.dedupe import dedupe_latest  # noqa: E402
from common.dq import DQCheck, apply_dq_checks  # noqa: E402
from common.silver_writer import write_quarantine_table, write_silver_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402

VALID_STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
VALID_CHANNELS = ["ONLINE", "STORE"]


def main():
    spark = get_spark_session("silver_transform_orders")

    # Load-bearing: bronze.orders is append-only across runs (see
    # bronze/extract_orders.py), so a re-run or backfill overlapping an
    # already-extracted order_ts window can put the same order_id in
    # Bronze twice.
    bronze_orders = dedupe_latest(spark.table("lakehouse.bronze.orders"), key_cols=["order_id"])
    silver_customers = spark.table("lakehouse.silver.customers").select(
        F.col("customer_id").alias("_known_customer_id")
    )
    silver_fx = spark.table("lakehouse.silver.exchange_rates").select(
        F.col("rate_date"), F.col("quote_currency"), F.col("rate")
    )

    with_fk_check = bronze_orders.join(
        silver_customers,
        bronze_orders.customer_id == silver_customers._known_customer_id,
        how="left",
    )

    with_fx = with_fk_check.join(
        silver_fx,
        (with_fk_check.order_date == silver_fx.rate_date)
        & (with_fk_check.currency_code == silver_fx.quote_currency),
        how="left",
    ).withColumn(
        "fx_rate_to_usd",
        F.when(F.col("currency_code") == "USD", F.lit(1.0)).otherwise(F.col("rate")),
    )

    transformed = with_fx.withColumn(
        "order_total_usd", F.round(F.col("order_total") / F.col("fx_rate_to_usd"), 2)
    ).withColumn(
        "shipping_cost_usd", F.round(F.col("shipping_cost") / F.col("fx_rate_to_usd"), 2)
    )

    checks = [
        DQCheck("order_id_not_null", F.col("order_id").isNotNull()),
        DQCheck("customer_id_exists", F.col("_known_customer_id").isNotNull()),
        DQCheck("order_status_valid", F.col("order_status").isin(VALID_STATUSES)),
        DQCheck("sales_channel_valid", F.col("sales_channel").isin(VALID_CHANNELS)),
        DQCheck("order_total_non_negative", F.col("order_total") >= 0),
        DQCheck("fx_conversion_resolved", F.col("order_total_usd").isNotNull()),
    ]
    good, quarantined = apply_dq_checks(transformed, checks)
    good = good.drop("_known_customer_id", "rate_date", "quote_currency", "rate")
    quarantined = quarantined.drop("_known_customer_id", "rate_date", "quote_currency", "rate")

    good_count, bad_count = good.count(), quarantined.count()
    print(f"[transform_orders] {good_count:,} rows passed, {bad_count:,} quarantined")

    write_silver_table(good, "orders", business_date_col="order_date")
    if bad_count > 0:
        write_quarantine_table(quarantined, "orders")

    spark.stop()


if __name__ == "__main__":
    main()
