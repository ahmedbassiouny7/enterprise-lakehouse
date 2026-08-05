"""
Gold: product_performance. Grain: one row per product, but the metrics
are RELATIVE — rank within category and % of category revenue — computed
via window functions partitioned by category. This is the BI-facing
"what are our top sellers per category" table, distinct from
inventory_summary.py's flat catalog+lifetime-sales view.

Products with zero sales get rank = NULL (dense_rank over a revenue-only
window naturally excludes them unless explicitly included) — handled by
building the ranking on all products via a left join first, so a
never-sold product still appears in the table (worth knowing "this exists
and has sold nothing," not just silently absent from a performance view).

Run:
    docker exec master spark-submit /opt/spark-jobs/gold/product_performance.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyspark.sql.functions as F  # noqa: E402
from pyspark.sql import Window  # noqa: E402

from common.gold_writer import write_gold_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402


def main():
    spark = get_spark_session("gold_product_performance")

    products = spark.table("lakehouse.silver.products")
    items = spark.table("lakehouse.silver.order_items")

    sales_agg = items.groupBy("product_id").agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("line_total_usd").alias("revenue_usd"),
    )

    base = (
        products.select("product_id", "product_name", "category")
        .join(sales_agg, "product_id", how="left")
        .withColumn("units_sold", F.coalesce(F.col("units_sold"), F.lit(0)))
        .withColumn("revenue_usd", F.round(F.coalesce(F.col("revenue_usd"), F.lit(0.0)), 2))
    )

    category_totals = base.groupBy("category").agg(
        F.sum("revenue_usd").alias("category_total_revenue_usd")
    )

    rank_window = Window.partitionBy("category").orderBy(F.col("revenue_usd").desc())

    result = (
        base.join(category_totals, "category")
        .withColumn("rank_in_category", F.dense_rank().over(rank_window))
        .withColumn(
            "pct_of_category_revenue",
            F.when(
                F.col("category_total_revenue_usd") > 0,
                F.round(F.col("revenue_usd") / F.col("category_total_revenue_usd") * 100, 2),
            ).otherwise(F.lit(0.0)),
        )
        .select(
            "product_id",
            "product_name",
            "category",
            "units_sold",
            "revenue_usd",
            "rank_in_category",
            "pct_of_category_revenue",
        )
    )

    print(f"[product_performance] {result.count():,} rows (one per product)")
    write_gold_table(result, "product_performance")
    spark.stop()


if __name__ == "__main__":
    main()
