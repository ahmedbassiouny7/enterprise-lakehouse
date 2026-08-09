"""Bronze extraction for exchange_rates from the CSV source.

Incremental by rate_date. The source is a full CSV rewritten by the
generator each time (see generator/generate_and_load.py), not an
appending file — there's no way to partially read a CSV — so
"incremental" here means: read the whole file, but only keep and append
rows with rate_date past the last watermark, instead of re-writing
Bronze's entire history every run.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyspark.sql.functions as F  # noqa: E402

from common.bronze_writer import add_audit_columns, append_bronze_table  # noqa: E402
from common.incremental import get_watermark, set_watermark  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402

SOURCE_NAME = "exchange-rates-csv"
CSV_PATH = "file:///opt/spark-jobs-data/exchange_rates.csv"

FX_SCHEMA = "rate_date DATE, base_currency STRING, quote_currency STRING, rate DECIMAL(12,6)"
WATERMARK_COL = "rate_date"


def main():
    spark = get_spark_session("bronze_extract_fx_rates")

    print(f"[extract_fx_rates] Reading exchange_rates from {CSV_PATH}...")
    df = spark.read.csv(CSV_PATH, header=True, schema=FX_SCHEMA)

    watermark = get_watermark(spark, "exchange_rates")
    if watermark is None:
        print("[extract_fx_rates] no watermark on record, full extract (first run)")
    else:
        print(f"[extract_fx_rates] filtering to {WATERMARK_COL} > {watermark}")
        df = df.filter(F.col(WATERMARK_COL) > F.lit(watermark).cast("date"))

    row_count = df.count()
    print(f"[extract_fx_rates]   {row_count:,} new rows")

    if row_count == 0:
        print("[extract_fx_rates]   nothing new — skipping write, watermark unchanged")
        print("[extract_fx_rates] Done.")
        spark.stop()
        return

    df = add_audit_columns(df, SOURCE_NAME)
    append_bronze_table(df, "exchange_rates")

    new_watermark = df.agg({WATERMARK_COL: "max"}).collect()[0][0]
    set_watermark(spark, "exchange_rates", str(new_watermark))
    print(f"[extract_fx_rates]   Appended to lakehouse.bronze.exchange_rates; watermark now {new_watermark}")
    print("[extract_fx_rates] Done.")
    spark.stop()


if __name__ == "__main__":
    main()
