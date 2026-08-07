"""Gold inventory_summary aggregation by product."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyspark.sql.functions as F  # noqa: E402

from common.gold_writer import write_gold_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402


def main():
    spark = get_spark_session("gold_inventory_summary")

    products = spark.table("lakehouse.silver.products")
    items = spark.table("lakehouse.silver.order_items")

    sales_agg = items.groupBy("product_id").agg(
        F.sum("quantity").alias("lifetime_units_sold"),
        F.sum("line_total_usd").alias("lifetime_revenue_usd"),
    )

    result = (
        products.join(sales_agg, "product_id", how="left")
        .withColumn("lifetime_units_sold", F.coalesce(F.col("lifetime_units_sold"), F.lit(0)))
        .withColumn(
            "lifetime_revenue_usd",
            F.round(F.coalesce(F.col("lifetime_revenue_usd"), F.lit(0.0)), 2),
        )
        .withColumn(
            "margin_pct",
            F.round((F.col("list_price") - F.col("unit_cost")) / F.col("list_price") * 100, 2),
        )
        .select(
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "is_active",
            "unit_cost",
            "list_price",
            "margin_pct",
            "lifetime_units_sold",
            "lifetime_revenue_usd",
        )
    )

    print(f"[inventory_summary] {result.count():,} rows (one per product)")
    write_gold_table(result, "inventory_summary")
    spark.stop()


if __name__ == "__main__":
    main()
