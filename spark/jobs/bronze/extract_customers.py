"""Bronze extraction for customers from MySQL.

Incremental by customer_id (monotonic surrogate PK). This table has no
updated_at column, so this only picks up brand-new customers — never a
profile edit to an existing customer_id (e.g. is_active flipping, or a
city change). Closing that gap needs either an updated_at column on the
source table or CDC. See docs/design-decisions.md, "Incremental
extraction and its limits."

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

SOURCE_NAME = "mysql-customers"
JDBC_URL = (
    f"jdbc:mysql://mysql-customers:3306/{os.environ.get('MYSQL_CUSTOMERS_DB', 'customers_db')}"
)
JDBC_PROPS = {
    "user": os.environ.get("MYSQL_CUSTOMERS_USER", "customers_app"),
    "password": os.environ.get("MYSQL_CUSTOMERS_PASSWORD", "customers_app_pw"),
    "driver": "com.mysql.cj.jdbc.Driver",
}
WATERMARK_COL = "customer_id"


def main():
    spark = get_spark_session("bronze_extract_customers")

    watermark = get_watermark(spark, "customers")
    if watermark is None:
        print("[extract_customers] no watermark on record, full extract (first run)")
        dbtable = "customers"
    else:
        print(f"[extract_customers] extracting rows where {WATERMARK_COL} > {watermark}")
        dbtable = f"(SELECT * FROM customers WHERE {WATERMARK_COL} > {watermark}) AS t"

    df = spark.read.jdbc(url=JDBC_URL, table=dbtable, properties=JDBC_PROPS)
    row_count = df.count()
    print(f"[extract_customers]   {row_count:,} new rows read")

    if row_count == 0:
        print("[extract_customers]   nothing new — skipping write, watermark unchanged")
        print("[extract_customers] Done.")
        spark.stop()
        return

    df = add_audit_columns(df, SOURCE_NAME)
    append_bronze_table(df, "customers")

    new_watermark = df.agg({WATERMARK_COL: "max"}).collect()[0][0]
    set_watermark(spark, "customers", str(new_watermark))
    print(f"[extract_customers]   Appended to lakehouse.bronze.customers; watermark now {new_watermark}")
    print("[extract_customers] Done.")
    spark.stop()


if __name__ == "__main__":
    main()
