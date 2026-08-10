"""Bronze extraction for orders and order_items from Postgres.

Incremental: each table uses a watermark column (see common/incremental.py)
so a run only pulls rows written since the last successful extraction,
rather than re-reading the entire source table.

  orders        watermark column: order_ts       (the row's own timestamp)
  order_items   watermark column: order_item_id  (monotonic surrogate PK —
                order_items has no timestamp of its own; this assumes IDs
                are assigned in insertion order, true for the Postgres
                BIGINT PRIMARY KEY this table actually uses)

Neither watermark can detect an UPDATE to an already-extracted row (e.g.
an order's status changing after ingestion) — only new rows. See
docs/design-decisions.md, "Incremental extraction and its limits."

SQL note: the watermark filter below is built via f-string, not a bound
parameter — safe here because `watermark` comes from Iceberg's own
control table (common/incremental.py), never from external input.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.bronze_writer import add_audit_columns, append_bronze_table  # noqa: E402
from common.incremental import get_watermark, set_watermark  # noqa: E402
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

# table_name -> watermark column
TABLES = {
    "orders": "order_ts",
    "order_items": "order_item_id",
}


def extract_table(spark, table_name: str, watermark_col: str) -> None:
    watermark = get_watermark(spark, table_name)
    if watermark is None:
        print(f"[extract_orders] {table_name}: no watermark on record, full extract (first run)")
        dbtable = table_name
    else:
        print(f"[extract_orders] {table_name}: extracting rows where {watermark_col} > {watermark}")
        dbtable = f"(SELECT * FROM {table_name} WHERE {watermark_col} > '{watermark}') AS t"

    df = spark.read.jdbc(url=JDBC_URL, table=dbtable, properties=JDBC_PROPS)
    row_count = df.count()
    print(f"[extract_orders]   {row_count:,} new rows read from {table_name}")

    if row_count == 0:
        print(f"[extract_orders]   nothing new for {table_name} — skipping write, watermark unchanged")
        return

    df = add_audit_columns(df, SOURCE_NAME)
    append_bronze_table(df, table_name)

    new_watermark = df.agg({watermark_col: "max"}).collect()[0][0]
    set_watermark(spark, table_name, str(new_watermark))
    print(f"[extract_orders]   Appended to lakehouse.bronze.{table_name}; watermark now {new_watermark}")


def main():
    spark = get_spark_session("bronze_extract_orders")
    for table_name, watermark_col in TABLES.items():
        extract_table(spark, table_name, watermark_col)
    print("[extract_orders] Done.")
    spark.stop()


if __name__ == "__main__":
    main()
