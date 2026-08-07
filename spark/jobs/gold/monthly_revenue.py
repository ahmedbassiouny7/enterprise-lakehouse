"""Gold monthly_revenue aggregation by month."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyspark.sql.functions as F  # noqa: E402
from pyspark.sql import Window  # noqa: E402

from common.gold_writer import write_gold_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402


def main():
    spark = get_spark_session("gold_monthly_revenue")

    orders = spark.table("lakehouse.silver.orders")

    monthly = (
        orders.withColumn("year_month", F.date_format("order_date", "yyyy-MM"))
        .groupBy("year_month")
        .agg(
            F.sum("order_total_usd").alias("total_revenue_usd"),
            F.count("order_id").alias("order_count"),
            F.round(F.avg("order_total_usd"), 2).alias("avg_order_value_usd"),
        )
        .withColumn("total_revenue_usd", F.round("total_revenue_usd", 2))
    )

    w = Window.orderBy("year_month")
    result = monthly.withColumn(
        "prev_month_revenue_usd", F.lag("total_revenue_usd").over(w)
    ).withColumn(
        "mom_revenue_growth_pct",
        F.when(
            F.col("prev_month_revenue_usd").isNotNull() & (F.col("prev_month_revenue_usd") != 0),
            F.round(
                (F.col("total_revenue_usd") - F.col("prev_month_revenue_usd"))
                / F.col("prev_month_revenue_usd")
                * 100,
                2,
            ),
        ).otherwise(F.lit(None)),
    )

    print(f"[monthly_revenue] {result.count():,} rows (one per month)")
    write_gold_table(result, "monthly_revenue")
    spark.stop()


if __name__ == "__main__":
    main()
