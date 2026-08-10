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


# --- Iceberg-backed fixtures, for common/incremental.py only -------------
#
# incremental.py hardcodes its table as "lakehouse.control.watermarks" and
# does real Iceberg MERGE INTO / CREATE TABLE USING iceberg calls, so the
# plain `spark` fixture above (no Iceberg config at all) can't exercise it.
# Rather than requiring the full Docker stack (Hive Metastore + HDFS) just
# to unit-test get_watermark/set_watermark, this points a catalog also
# named "lakehouse" at Iceberg's HadoopCatalog against a local temp
# directory — no Hive Metastore involved, so it still runs without Docker.
#
# Requires the Iceberg runtime jar already downloaded to docker/spark-jars/
# (see that folder's README, needed for the real stack anyway) — tests
# using this fixture are skipped if it isn't there yet, rather than failing.
ICEBERG_JAR = os.path.join(REPO_ROOT, "docker", "spark-jars", "iceberg-spark-runtime-3.3_2.12-1.4.3.jar")


@pytest.fixture(scope="session")
def iceberg_spark(tmp_path_factory):
    if not os.path.exists(ICEBERG_JAR):
        pytest.skip(
            "Iceberg runtime jar not found — run the curl command in "
            "docker/spark-jars/README.md first (only needed for the "
            "incremental.py tests; everything else runs without it)."
        )
    warehouse = tmp_path_factory.mktemp("iceberg_warehouse")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("lakehouse-incremental-tests")
        .config("spark.jars", ICEBERG_JAR)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", f"file://{warehouse}")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sql("CREATE DATABASE IF NOT EXISTS lakehouse.control")
    yield session
    session.stop()


@pytest.fixture
def clean_watermarks(iceberg_spark):
    """Drop the watermark table before each test, since iceberg_spark is
    session-scoped and tests would otherwise see state left behind by
    whichever test ran before them."""
    iceberg_spark.sql("DROP TABLE IF EXISTS lakehouse.control.watermarks")
    yield iceberg_spark
