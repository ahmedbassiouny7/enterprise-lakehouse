"""Bronze extraction for customers from MySQL."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.bronze_writer import add_audit_columns, write_bronze_table  # noqa: E402
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


def main():
    spark = get_spark_session("bronze_extract_customers")

    print(f"[extract_customers] Reading customers from {SOURCE_NAME}...")
    df = spark.read.jdbc(url=JDBC_URL, table="customers", properties=JDBC_PROPS)
    row_count = df.count()
    print(f"[extract_customers]   {row_count:,} rows read")

    df = add_audit_columns(df, SOURCE_NAME)
    write_bronze_table(df, "customers")
    print("[extract_customers]   Wrote lakehouse.bronze.customers")
    print("[extract_customers] Done.")
    spark.stop()


if __name__ == "__main__":
    main()
