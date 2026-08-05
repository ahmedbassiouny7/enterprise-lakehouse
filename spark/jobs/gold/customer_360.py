"""
Gold: customer_360. Grain: one row per customer. Left-joins orders onto
the full customer list (not inner) so customers with zero orders still
appear with zeroed-out metrics — a "customer with no purchases yet" is a
real, meaningful row for retention/marketing use cases, not something to
silently drop.

Run:
    docker exec master spark-submit /opt/spark-jobs/gold/customer_360.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyspark.sql.functions as F  # noqa: E402

from common.gold_writer import write_gold_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402


def main():
    spark = get_spark_session("gold_customer_360")

    customers = spark.table("lakehouse.silver.customers")
    orders = spark.table("lakehouse.silver.orders")

    order_agg = orders.groupBy("customer_id").agg(
        F.count("order_id").alias("lifetime_order_count"),
        F.sum("order_total_usd").alias("lifetime_revenue_usd"),
        F.round(F.avg("order_total_usd"), 2).alias("avg_order_value_usd"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
    )

    result = (
        customers.join(order_agg, "customer_id", how="left")
        .withColumn("lifetime_order_count", F.coalesce(F.col("lifetime_order_count"), F.lit(0)))
        .withColumn(
            "lifetime_revenue_usd", F.round(F.coalesce(F.col("lifetime_revenue_usd"), F.lit(0.0)), 2)
        )
        .withColumn(
            "days_since_last_order", F.datediff(F.current_date(), F.col("last_order_date"))
        )
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "country",
            "city",
            "customer_segment",
            "is_active",
            "signup_date",
            "lifetime_order_count",
            "lifetime_revenue_usd",
            "avg_order_value_usd",
            "first_order_date",
            "last_order_date",
            "days_since_last_order",
        )
    )

    print(f"[customer_360] {result.count():,} rows (one per customer)")
    write_gold_table(result, "customer_360")
    spark.stop()


if __name__ == "__main__":
    main()
