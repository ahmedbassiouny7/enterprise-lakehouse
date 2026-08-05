"""
Bronze extraction: orders + order_items, from postgres-orders.

Reads via Spark's JDBC data source directly against Postgres — deliberately
NOT through Trino. Trino stays a pure query/virtualization layer; routing
scheduled ETL through it would add a network hop and lose Spark's native
JDBC read parallelism for zero benefit (see docs/design-decisions.md).

Run:
    docker exec master spark-submit /opt/spark-jobs/bronze/extract_orders.py

Single-connection JDBC read (no partitionColumn/numPartitions) — at this
project's scale (100K orders / ~165K order_items) a parallel partitioned
read isn't worth the added complexity. At real scale you'd add:
    .option("partitionColumn", "order_id")
    .option("lowerBound", <min>).option("upperBound", <max>)
    .option("numPartitions", 8)
with bounds fetched via a cheap MIN/MAX query first, not hardcoded.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.bronze_writer import add_audit_columns, write_bronze_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402

SOURCE_NAME = "postgres-orders"
JDBC_URL = (
    f"jdbc:postgresql://postgres-orders:5432/{os.environ.get('POSTGRES_ORDERS_DB', 'orders_db')}"
)
JDBC_PROPS = {
    "user": os.environ.get("POSTGRES_ORDERS_USER", "orders_app"),
    "password": os.environ.get("POSTGRES_ORDERS_PASSWORD", "orders_app_pw"),
    "driver": "org.postgresql.Driver",
}


def extract_table(spark, table_name: str) -> None:
    print(f"[extract_orders] Reading {table_name} from {SOURCE_NAME}...")
    df = spark.read.jdbc(url=JDBC_URL, table=table_name, properties=JDBC_PROPS)
    row_count = df.count()
    print(f"[extract_orders]   {row_count:,} rows read from {table_name}")

    df = add_audit_columns(df, SOURCE_NAME)
    write_bronze_table(df, table_name)
    print(f"[extract_orders]   Wrote lakehouse.bronze.{table_name}")


def main():
    spark = get_spark_session("bronze_extract_orders")
    extract_table(spark, "orders")
    extract_table(spark, "order_items")
    print("[extract_orders] Done.")
    spark.stop()


if __name__ == "__main__":
    main()
