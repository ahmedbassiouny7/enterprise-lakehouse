"""
Bronze extraction: exchange_rates.

KNOWN GAP, not an oversight: the project spec calls for exchange_rates to
come from a REST API, but that mock API isn't built yet (see
session-handoff-v2.md, "Open / unconfirmed"). This job reads the CSV
data-generator already produces for that payload as a stand-in, from the
same mounted path as products.csv. When the mock API exists, only this
job's read path changes (CSV read -> HTTP GET + JSON parse) — the audit
columns, write path, and partitioning strategy stay identical, and
_bronze_source below should change from "exchange-rates-csv" to whatever
the API is called, so lineage stays honest about where the data actually
came from at the time of each run.

Run:
    docker exec master spark-submit /opt/spark-jobs/bronze/extract_fx_rates.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.bronze_writer import add_audit_columns, write_bronze_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402

SOURCE_NAME = "exchange-rates-csv"  # update when the mock REST API replaces this
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
