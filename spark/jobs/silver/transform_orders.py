"""
Silver transform: orders.

Depends on silver.customers (referential check) and silver.exchange_rates
(USD conversion) — must run AFTER both of those, not in parallel with them.
transform_order_items.py depends on THIS job's output, so the DAG order is:
    {products, customers, exchange_rates} -> orders -> order_items

Currency conversion: the generator's exchange_rates are always base=USD,
quote=<currency>, meaning "1 USD = <rate> <currency>". An order_total in a
non-USD currency converts back to USD as order_total / rate. USD orders
need no conversion (rate is implicitly 1). This is a real business
decision worth stating explicitly: Gold-layer revenue metrics need one
consistent currency to sum across, and USD (the FX table's base) is the
natural choice rather than picking an arbitrary target currency.

Run:
    docker exec master spark-submit /opt/spark-jobs/silver/transform_orders.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyspark.sql.functions as F  # noqa: E402

from common.dq import DQCheck, apply_dq_checks  # noqa: E402
from common.silver_writer import write_quarantine_table, write_silver_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402

VALID_STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
VALID_CHANNELS = ["ONLINE", "STORE"]


def main():
    spark = get_spark_session("silver_transform_orders")

    bronze_orders = spark.table("lakehouse.bronze.orders")
    silver_customers = spark.table("lakehouse.silver.customers").select(
        F.col("customer_id").alias("_known_customer_id")
    )
    silver_fx = spark.table("lakehouse.silver.exchange_rates").select(
        F.col("rate_date"), F.col("quote_currency"), F.col("rate")
    )

    # Referential check against silver.customers, not bronze.customers —
    # an order referencing a customer that itself got quarantined in Silver
    # (bad email, invalid segment, etc.) should also be flagged, not treated
    # as valid just because it existed in raw Bronze.
    with_fk_check = bronze_orders.join(
        silver_customers,
        bronze_orders.customer_id == silver_customers._known_customer_id,
        how="left",
    )

    # USD conversion: left-join on (order_date, currency_code) for non-USD
    # orders; USD orders get rate=1 directly without needing a join match.
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
        # A non-USD order with no fx match for its (order_date, currency)
        # would silently divide by a null rate and produce a null USD
        # total — catch that explicitly instead of letting it slip through
        # as a silent null in a downstream revenue sum.
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
