"""Shared data-quality framework for Silver transforms."""
from dataclasses import dataclass
from typing import List, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass
class DQCheck:
    name: str
    condition: Column  # boolean expression; True = row passes this check


def apply_dq_checks(df: DataFrame, checks: List[DQCheck]) -> Tuple[DataFrame, DataFrame]:
    """Split rows into good and quarantine outputs."""
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
