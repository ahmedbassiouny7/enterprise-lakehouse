"""
Shared SparkSession factory for all Bronze/Silver/Gold jobs.

Why this exists: docker/spark-jars/README.md documents five --conf flags
that must be passed on every spark-submit because the hadoop-hive-spark
image's baked-in spark-defaults.conf sets spark.master=yarn and has no
Iceberg catalog registered. Repeating those flags on every submit command
(and every SparkSubmitOperator call in Airflow later) is exactly the kind
of thing that gets forgotten once and produces a confusing failure. This
module bakes them into the SparkSession itself instead, so a job only
ever needs: spark-submit /opt/spark-jobs/bronze/extract_orders.py

Values still come from environment variables where the docker-compose
.env file already defines them (ICEBERG_WAREHOUSE, SPARK_MASTER_*), so
this file has no hardcoded values that duplicate .env — if those change,
this doesn't need editing.
"""
import os

from pyspark.sql import SparkSession

# Iceberg catalog name — matches docker/spark-jars/README.md exactly.
# NOTE: this is deliberately named "lakehouse", not "iceberg". Trino's
# Iceberg catalog is named "iceberg" (from its catalog properties
# filename) — different name, same underlying Hive Metastore
# (thrift://master:9083) and same warehouse path, so both engines see
# the same tables. Don't "fix" this to match Trino's name; it's fine
# for two engines to use different local names for the same catalog.
ICEBERG_CATALOG = "lakehouse"

SPARK_MASTER_URL = f"spark://{os.environ.get('SPARK_MASTER_HOST', 'master')}:{os.environ.get('SPARK_MASTER_RPC_PORT', '7077')}"
HIVE_METASTORE_URI = "thrift://master:9083"  # HARDCODED IN IMAGE — see docs/design-decisions.md
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "hdfs://master/warehouse/iceberg")


def get_spark_session(app_name: str) -> SparkSession:
    """Build (or fetch) the SparkSession every Bronze/Silver/Gold job should use."""
    spark = (
        SparkSession.builder.appName(app_name)
        .master(SPARK_MASTER_URL)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hive")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.uri", HIVE_METASTORE_URI)
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .getOrCreate()
    )
    # Idempotent — cheap to run on every job rather than assuming setup ran
    # first. quarantine holds DQ-failed rows from Silver transforms (see
    # common/dq.py); kept as its own database, not a suffix on silver
    # tables, so it can have different retention/access rules later
    # without touching the silver schema.
    for db in ("bronze", "silver", "gold", "quarantine"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{db}")
    return spark
