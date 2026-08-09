"""Airflow DAG for the Bronze/Silver/Gold pipeline."""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.utils.task_group import TaskGroup

SPARK_MASTER_CONN_ID = "spark_default"
SPARK_JOB_ROOT = "/opt/airflow/spark_jobs"


def _spark_submit_task(task_id: str, path: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"{SPARK_JOB_ROOT}/{path}",
        conn_id=SPARK_MASTER_CONN_ID,
        deploy_mode="client",
        name=f"lakehouse_{task_id}",
        verbose=True,
        application_args=[],
    )

# (job_name, relative path under spark/jobs/) — kept as one list per layer
# so task-group wiring below stays a simple loop instead of repeated
# boilerplate per task.
BRONZE_JOBS = [
    ("orders", "bronze/extract_orders.py"),
    ("customers", "bronze/extract_customers.py"),
    ("products", "bronze/extract_products.py"),
    ("fx_rates", "bronze/extract_fx_rates.py"),
]
SILVER_INDEPENDENT_JOBS = [
    ("products", "silver/transform_products.py"),
    ("customers", "silver/transform_customers.py"),
    ("exchange_rates", "silver/transform_exchange_rates.py"),
]
GOLD_JOBS = [
    ("daily_sales", "gold/daily_sales.py"),
    ("customer_360", "gold/customer_360.py"),
    ("monthly_revenue", "gold/monthly_revenue.py"),
    ("inventory_summary", "gold/inventory_summary.py"),
    ("product_performance", "gold/product_performance.py"),
]

# Per-table quarantine thresholds; each table carries a different downstream risk.
DQ_THRESHOLDS_PCT = {
    "products": 3.0,
    "customers": 1.5,
    "exchange_rates": 0.5,
    "orders": 1.0,
    "order_items": 1.0,
}


def _is_table_not_found(exc: Exception) -> bool:
    """Treat 'table not found' as a non-fatal quarantine condition."""
    msg = str(exc)
    return "TABLE_NOT_FOUND" in msg or "does not exist" in msg


def _check_dq_quarantine(**context):
    """Fail the DAG if a Silver table exceeds its quarantine threshold."""
    hook = TrinoHook(trino_conn_id="trino_default")
    log = context["ti"].log
    violations = []

    for table, threshold_pct in DQ_THRESHOLDS_PCT.items():
        try:
            silver_count = hook.get_first(f"SELECT count(*) FROM iceberg.silver.{table}")[0]
        except Exception as e:
            # Silver tables are guaranteed to exist by this point in the
            # DAG (silver_order_items_task has already succeeded) — there
            # is no legitimate reason this query fails. Anything here is
            # a real problem (auth, connection, permissions, typo'd table
            # name) and must not be silently treated as "0 rows".
            raise RuntimeError(
                f"[dq_quality_gate] could not query iceberg.silver.{table} — "
                f"treating as a hard failure rather than defaulting to 0 "
                f"rows, since a Silver table missing at this point in the "
                f"DAG is never expected: {e}"
            ) from e

        try:
            quarantine_count = hook.get_first(
                f"SELECT count(*) FROM iceberg.quarantine.{table}"
            )[0]
        except Exception as e:
            if _is_table_not_found(e):
                # Quarantine table genuinely doesn't exist -> that Silver
                # job quarantined zero rows (write_quarantine_table is only
                # called when there are rows to write). Not an error.
                quarantine_count = 0
            else:
                # Anything else (auth failure, connection drop, permissions)
                # must not be silently reinterpreted as "zero quarantined".
                raise RuntimeError(
                    f"[dq_quality_gate] could not query "
                    f"iceberg.quarantine.{table} and the failure doesn't "
                    f"look like 'table not found' — refusing to default "
                    f"this to 0 rows quarantined: {e}"
                ) from e

        total = silver_count + quarantine_count
        pct = round((quarantine_count / total) * 100, 2) if total > 0 else 0.0

        if pct > threshold_pct:
            log.error(
                "[dq_quality_gate] %s: %s/%s rows quarantined (%.2f%%) — "
                "ABOVE %s%% threshold",
                table,
                quarantine_count,
                total,
                pct,
                threshold_pct,
            )
            violations.append(f"{table}: {pct:.2f}% quarantined (threshold {threshold_pct}%)")
        else:
            log.info(
                "[dq_quality_gate] %s: %s/%s rows quarantined (%.2f%%) — within %s%% threshold",
                table,
                quarantine_count,
                total,
                pct,
                threshold_pct,
            )

    if violations:
        raise RuntimeError(
            "[dq_quality_gate] Gold blocked — quarantine rate exceeded threshold for: "
            + "; ".join(violations)
        )


def _validate_trino_catalog(**context):
    """Verify each Gold table is present and non-empty via Trino."""
    hook = TrinoHook(trino_conn_id="trino_default")
    log = context["ti"].log
    problems = []

    for job_name, _ in GOLD_JOBS:
        try:
            count = hook.get_first(f"SELECT count(*) FROM iceberg.gold.{job_name}")[0]
        except Exception as e:
            problems.append(f"{job_name}: query failed ({e})")
            continue
        if count == 0:
            problems.append(f"{job_name}: 0 rows")
        else:
            log.info("[validate_trino_catalog] gold.%s: %s rows, reachable via Trino", job_name, count)

    if problems:
        raise RuntimeError(f"Gold validation failed: {problems}")


with DAG(
    dag_id="lakehouse_pipeline",
    description="Bronze -> Silver -> Gold Medallion pipeline for the Enterprise Lakehouse project",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["lakehouse", "medallion"],
) as dag:

    with TaskGroup("bronze") as bronze_group:
        bronze_tasks = {
            name: _spark_submit_task(f"extract_{name}", path)
            for name, path in BRONZE_JOBS
        }

    with TaskGroup("silver") as silver_group:
        silver_independent_tasks = {
            name: _spark_submit_task(f"transform_{name}", path)
            for name, path in SILVER_INDEPENDENT_JOBS
        }

        silver_orders_task = _spark_submit_task("transform_orders", "silver/transform_orders.py")
        silver_order_items_task = _spark_submit_task("transform_order_items", "silver/transform_order_items.py")

        # orders depends on customer and FX lookups
        [silver_independent_tasks["customers"], silver_independent_tasks["exchange_rates"]] >> silver_orders_task
        # order_items depends on orders and products
        [silver_orders_task, silver_independent_tasks["products"]] >> silver_order_items_task

    dq_quality_gate = PythonOperator(
        task_id="dq_quality_gate",
        python_callable=_check_dq_quarantine,
    )

    with TaskGroup("gold") as gold_group:
        gold_tasks = [
            _spark_submit_task(f"compute_{name}", path)
            for name, path in GOLD_JOBS
        ]

    validate_trino_catalog = PythonOperator(
        task_id="validate_trino_catalog",
        python_callable=_validate_trino_catalog,
    )

    # Bronze -> matching independent Silver job
    bronze_tasks["orders"] >> silver_orders_task
    bronze_tasks["customers"] >> silver_independent_tasks["customers"]
    bronze_tasks["products"] >> silver_independent_tasks["products"]
    bronze_tasks["fx_rates"] >> silver_independent_tasks["exchange_rates"]

    # All of Silver -> DQ gate -> all of Gold -> Trino validation
    silver_order_items_task >> dq_quality_gate >> list(gold_tasks) >> validate_trino_catalog