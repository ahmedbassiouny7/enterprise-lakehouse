"""Bronze extraction for products from the flat-file CSV.

Deliberately NOT incremental, unlike orders/order_items/customers/
exchange_rates. Two reasons together, not one alone:
  1. No usable watermark column — products has no updated_at, and unlike
     customers/order_items it also has no monotonic surrogate that would
     stand in for one (product_id is stable/reused, not append-only, once
     is_active toggles are considered).
  2. It's a small dimension table (~2,000 rows) — a full re-read costs
     nothing next to a 100k-row fact table, so there's no performance
     problem incremental extraction would be solving here.
See docs/design-decisions.md, "Incremental extraction and its limits."
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.bronze_writer import add_audit_columns, write_bronze_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402

SOURCE_NAME = "products-csv"
CSV_PATH = "file:///opt/spark-jobs-data/products.csv"

PRODUCTS_SCHEMA = (
    "product_id INT, product_name STRING, category STRING, subcategory STRING, "
    "brand STRING, unit_cost DECIMAL(10,2), list_price DECIMAL(10,2), is_active BOOLEAN"
)


def main():
    spark = get_spark_session("bronze_extract_products")

    print(f"[extract_products] Reading products from {CSV_PATH}...")
    df = spark.read.csv(CSV_PATH, header=True, schema=PRODUCTS_SCHEMA)
    row_count = df.count()
    print(f"[extract_products]   {row_count:,} rows read")

    df = add_audit_columns(df, SOURCE_NAME)
    write_bronze_table(df, "products")
    print("[extract_products]   Wrote lakehouse.bronze.products")
    print("[extract_products] Done.")
    spark.stop()


if __name__ == "__main__":
    main()
