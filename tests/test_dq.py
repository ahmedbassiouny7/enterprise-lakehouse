"""Unit tests for spark/jobs/common/dq.py — apply_dq_checks."""
import pyspark.sql.functions as F

from common.dq import DQCheck, apply_dq_checks


def test_all_rows_pass_when_all_checks_pass(spark):
    df = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "amount"])
    checks = [
        DQCheck("id_not_null", F.col("id").isNotNull()),
        DQCheck("amount_positive", F.col("amount") > 0),
    ]
    good, quarantined = apply_dq_checks(df, checks)

    assert good.count() == 3
    assert quarantined.count() == 0
    # The good/quarantine split must not leak the internal flag columns
    # apply_dq_checks builds while evaluating each check.
    assert "_dq_failed_checks" not in good.columns
    assert "_dq_failed_checks" not in quarantined.columns


def test_rows_split_correctly_on_a_single_failing_check(spark):
    df = spark.createDataFrame([(1, 10), (2, -5), (3, 30)], ["id", "amount"])
    checks = [DQCheck("amount_positive", F.col("amount") > 0)]
    good, quarantined = apply_dq_checks(df, checks)

    assert sorted(r["id"] for r in good.collect()) == [1, 3]
    assert sorted(r["id"] for r in quarantined.collect()) == [2]


def test_row_failing_multiple_checks_lands_in_quarantine_exactly_once(spark):
    # id=None fails both checks — it must appear in quarantine once, not
    # once per failed check (a naive union-of-failures implementation
    # would duplicate it).
    df = spark.createDataFrame([(1, 10), (None, -5), (3, 30)], ["id", "amount"])
    checks = [
        DQCheck("id_not_null", F.col("id").isNotNull()),
        DQCheck("amount_positive", F.col("amount") > 0),
    ]
    good, quarantined = apply_dq_checks(df, checks)

    assert good.count() == 2
    assert quarantined.count() == 1


def test_no_checks_means_every_row_passes(spark):
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    good, quarantined = apply_dq_checks(df, checks=[])

    assert good.count() == 2
    assert quarantined.count() == 0
