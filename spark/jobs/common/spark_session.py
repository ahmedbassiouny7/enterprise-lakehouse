"""Shared SparkSession setup for all jobs."""
import os

from pyspark.sql import SparkSession

# Iceberg catalog name, separate from Trino's external catalog name.
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
    # Ensure all required Iceberg databases exist.
    # "control" holds pipeline-internal bookkeeping (extraction watermarks
    # — see common/incremental.py), not business data; kept as its own
    # database rather than dumped into bronze/ so it's obvious it isn't a
    # source table if someone browses the catalog.
    for db in ("bronze", "silver", "gold", "quarantine", "control"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{db}")
    return spark
