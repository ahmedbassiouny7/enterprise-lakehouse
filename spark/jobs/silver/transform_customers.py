"""Silver transform for customers."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyspark.sql.functions as F  # noqa: E402

from common.dedupe import dedupe_latest  # noqa: E402
from common.dq import DQCheck, apply_dq_checks  # noqa: E402
from common.silver_writer import write_quarantine_table, write_silver_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402

EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
VALID_SEGMENTS = ["VIP", "RETAIL", "WHOLESALE"]


def main():
    spark = get_spark_session("silver_transform_customers")

    bronze = spark.table("lakehouse.bronze.customers")

    # Now load-bearing: bronze.customers is append-only across runs (see
    # bronze/extract_customers.py), so the same customer_id can appear
    # more than once here if extraction ever re-reads an already-seen id.
    deduped = dedupe_latest(bronze, key_cols=["customer_id"])

    checks = [
        DQCheck("customer_id_not_null", F.col("customer_id").isNotNull()),
        DQCheck("email_well_formed", F.col("email").rlike(EMAIL_REGEX)),
        DQCheck("signup_date_not_null", F.col("signup_date").isNotNull()),
        DQCheck(
            "signup_date_not_future",
            F.col("signup_date") <= F.current_date(),
        ),
        DQCheck("customer_segment_valid", F.col("customer_segment").isin(VALID_SEGMENTS)),
    ]
    good, quarantined = apply_dq_checks(deduped, checks)

    good_count, bad_count = good.count(), quarantined.count()
    print(f"[transform_customers] {good_count:,} rows passed, {bad_count:,} quarantined")

    write_silver_table(good, "customers", business_date_col="signup_date")
    if bad_count > 0:
        write_quarantine_table(quarantined, "customers")

    spark.stop()


if __name__ == "__main__":
    main()