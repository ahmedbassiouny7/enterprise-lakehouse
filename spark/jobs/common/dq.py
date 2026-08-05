"""
Shared data-quality framework for Silver transforms.

Design: rows that fail a check are NOT silently dropped. They're routed to
a quarantine table (lakehouse.quarantine.<table>) with a record of exactly
which check(s) failed, and only rows that pass everything reach Silver.
This is deliberate — a pipeline that quietly drops "bad" rows makes data
loss invisible, and "how many rows did DQ reject and why" is exactly the
kind of question a senior interviewer asks. Quarantining answers it with a
queryable table instead of a shrug.

Usage:
    from common.dq import DQCheck, apply_dq_checks
    import pyspark.sql.functions as F

    checks = [
        DQCheck("customer_id_not_null", F.col("customer_id").isNotNull()),
        DQCheck("order_total_non_negative", F.col("order_total") >= 0),
    ]
    good_df, quarantined_df = apply_dq_checks(df, checks)
"""
from dataclasses import dataclass
from typing import List, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass
class DQCheck:
    name: str
    condition: Column  # boolean expression; True = row passes this check


def apply_dq_checks(df: DataFrame, checks: List[DQCheck]) -> Tuple[DataFrame, DataFrame]:
    """Returns (good_df, quarantine_df). quarantine_df gains a
    `_dq_failed_checks` column: a comma-separated list of every check name
    that failed for that row (a row can fail more than one check at once —
    worth seeing all of them, not just the first)."""
    working = df
    flag_cols = []
    for check in checks:
        flag_col = f"_dq_flag_{check.name}"
        working = working.withColumn(
            flag_col, F.when(check.condition, F.lit(None)).otherwise(F.lit(check.name))
        )
        flag_cols.append(flag_col)

    working = working.withColumn(
        "_dq_failed_checks",
        F.array_join(F.array_except(F.array(*flag_cols), F.array(F.lit(None))), ","),
    )

    good_df = working.filter(F.col("_dq_failed_checks") == "").drop(
        *flag_cols, "_dq_failed_checks"
    )
    quarantine_df = working.filter(F.col("_dq_failed_checks") != "").drop(*flag_cols)

    return good_df, quarantine_df
