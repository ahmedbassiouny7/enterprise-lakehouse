"""
Bronze extraction: products, from the flat-file CSV.

products.csv is written by data-generator to ./generator/output, which is
bind-mounted read-only into master at /opt/spark-jobs-data (see
docker-compose.yml). This models a genuine flat-file source per the
project spec, distinct from the two live JDBC sources.

Run:
    docker exec master spark-submit /opt/spark-jobs/bronze/extract_products.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.bronze_writer import add_audit_columns, write_bronze_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402

SOURCE_NAME = "products-csv"
CSV_PATH = "file:///opt/spark-jobs-data/products.csv"

# Explicit schema rather than inferSchema=True: inferSchema forces Spark to
# do a full pre-read pass over the file just to guess types, and silently
# guessing wrong (e.g. is_active landing as a string "1"/"0" instead of a
# proper flag) is exactly the kind of thing that should be caught at
# extraction time, not discovered three layers downstream in Gold.
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
