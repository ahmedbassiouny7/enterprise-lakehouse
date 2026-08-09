"""Silver transform for exchange_rates."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyspark.sql.functions as F  # noqa: E402

from common.dedupe import dedupe_latest  # noqa: E402
from common.dq import DQCheck, apply_dq_checks  # noqa: E402
from common.silver_writer import write_quarantine_table, write_silver_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402


def main():
    spark = get_spark_session("silver_transform_exchange_rates")

    bronze = spark.table("lakehouse.bronze.exchange_rates")

    # Now load-bearing: bronze.exchange_rates is append-only across runs
    # (see bronze/extract_fx_rates.py), so the same (rate_date,
    # base_currency, quote_currency) can appear more than once if
    # extraction ever re-reads an already-seen date.
    key_cols = ["rate_date", "base_currency", "quote_currency"]
    deduped = dedupe_latest(bronze, key_cols=key_cols)

    checks = [
        DQCheck("rate_date_not_null", F.col("rate_date").isNotNull()),
        DQCheck("rate_positive", F.col("rate") > 0),
        DQCheck("base_currency_is_usd", F.col("base_currency") == "USD"),
    ]
    good, quarantined = apply_dq_checks(deduped, checks)

    good_count, bad_count = good.count(), quarantined.count()
    print(f"[transform_exchange_rates] {good_count:,} rows passed, {bad_count:,} quarantined")

    write_silver_table(good, "exchange_rates", business_date_col="rate_date")
    if bad_count > 0:
        write_quarantine_table(quarantined, "exchange_rates")

    spark.stop()


if __name__ == "__main__":
    main()