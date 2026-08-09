"""Silver transform for products."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyspark.sql.functions as F  # noqa: E402

from common.dedupe import dedupe_latest  # noqa: E402
from common.dq import DQCheck, apply_dq_checks  # noqa: E402
from common.silver_writer import write_quarantine_table, write_silver_table  # noqa: E402
from common.spark_session import get_spark_session  # noqa: E402


def main():
    spark = get_spark_session("silver_transform_products")

    bronze = spark.table("lakehouse.bronze.products")

    # Defensive, not currently load-bearing: bronze.products is still a
    # full-overwrite snapshot each run (see bronze/extract_products.py —
    # products deliberately stayed non-incremental), so a given run's
    # bronze.products can't contain two _bronze_ingested_at values for one
    # product_id today. Kept here so this stays correct if products ever
    # does move to incremental extraction.
    deduped = dedupe_latest(bronze, key_cols=["product_id"])

    checks = [
        DQCheck("product_id_not_null", F.col("product_id").isNotNull()),
        DQCheck("product_name_not_null", F.col("product_name").isNotNull()),
        DQCheck("unit_cost_non_negative", F.col("unit_cost") >= 0),
        DQCheck("list_price_non_negative", F.col("list_price") >= 0),
        DQCheck("list_price_gte_unit_cost", F.col("list_price") >= F.col("unit_cost")),
    ]
    good, quarantined = apply_dq_checks(deduped, checks)

    good_count, bad_count = good.count(), quarantined.count()
    print(f"[transform_products] {good_count:,} rows passed, {bad_count:,} quarantined")

    write_silver_table(good, "products", business_date_col="_bronze_ingested_at")
    if bad_count > 0:
        write_quarantine_table(quarantined, "products")

    spark.stop()


if __name__ == "__main__":
    main()