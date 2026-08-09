"""Pytest fixtures shared across the test suite.

Local-only unit tests: these spin up a local[1] SparkSession with no
Iceberg/Hive config, and test the pure DataFrame logic in
spark/jobs/common/ directly. They don't touch Docker, HDFS, or the
Iceberg catalog, so they run anywhere PySpark is installed (see
tests/requirements.txt) — the Docker stack does not need to be up.
"""
import os
import sys

import pytest
from pyspark.sql import SparkSession

# spark/jobs/common/*.py modules import each other as `from common.x import
# y` (see e.g. spark/jobs/silver/transform_orders.py) — that only resolves
# because each job script does sys.path.insert(0, "..") relative to its own
# file before importing anything. Tests aren't run from inside spark/jobs/,
# so do the equivalent here once, against the repo's real spark/jobs path.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPARK_JOBS_DIR = os.path.join(REPO_ROOT, "spark", "jobs")
if SPARK_JOBS_DIR not in sys.path:
    sys.path.insert(0, SPARK_JOBS_DIR)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("lakehouse-unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
