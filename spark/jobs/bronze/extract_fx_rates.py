"""Bronze extraction for exchange_rates from the CSV source."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.bronze_writer import add_audit_columns, write_bronze_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402

SOURCE_NAME = "exchange-rates-csv"
CSV_PATH = "file:///opt/spark-jobs-data/exchange_rates.csv"

FX_SCHEMA = "rate_date DATE, base_currency STRING, quote_currency STRING, rate DECIMAL(12,6)"


def main():
    spark = get_spark_session("bronze_extract_fx_rates")

    print(f"[extract_fx_rates] Reading exchange_rates from {CSV_PATH}...")
    df = spark.read.csv(CSV_PATH, header=True, schema=FX_SCHEMA)
    row_count = df.count()
    print(f"[extract_fx_rates]   {row_count:,} rows read")

    df = add_audit_columns(df, SOURCE_NAME)
    write_bronze_table(df, "exchange_rates")
    print("[extract_fx_rates]   Wrote lakehouse.bronze.exchange_rates")
    print("[extract_fx_rates] Done.")
    spark.stop()


if __name__ == "__main__":
    main()
