"""Gold daily_sales aggregation by order_date and sales_channel."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyspark.sql.functions as F  # noqa: E402

from common.gold_writer import write_gold_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402


def main():
    spark = get_spark_session("gold_daily_sales")

    orders = spark.table("lakehouse.silver.orders")
    items = spark.table("lakehouse.silver.order_items")

    items_per_order = items.groupBy("order_id").agg(F.sum("quantity").alias("items_in_order"))

    enriched = orders.join(items_per_order, "order_id", how="left")

    result = (
        enriched.groupBy("order_date", "sales_channel")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("order_total_usd").alias("total_revenue_usd"),
            F.round(F.avg("order_total_usd"), 2).alias("avg_order_value_usd"),
            F.sum("items_in_order").alias("total_items_sold"),
            F.sum(F.when(F.col("order_status") == "RETURNED", 1).otherwise(0)).alias(
                "returned_order_count"
            ),
            F.sum(F.when(F.col("order_status") == "CANCELLED", 1).otherwise(0)).alias(
                "cancelled_order_count"
            ),
        )
        .withColumn("total_revenue_usd", F.round("total_revenue_usd", 2))
    )

    print(f"[daily_sales] {result.count():,} rows (date x channel grain)")
    write_gold_table(result, "daily_sales")
    spark.stop()


if __name__ == "__main__":
    main()
