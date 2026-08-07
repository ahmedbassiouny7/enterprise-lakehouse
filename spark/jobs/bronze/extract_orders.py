"""Bronze extraction for orders and order_items from Postgres."""
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
