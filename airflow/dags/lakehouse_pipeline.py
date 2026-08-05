"""
lakehouse_pipeline — Bronze -> Silver -> Gold, daily batch.

Execution model: every Spark task shells out to
    docker exec master spark-submit /opt/spark-jobs/<path>
rather than using SparkSubmitOperator directly from the Airflow container.
Why, and the real tradeoff that comes with it, is documented next to the
docker.sock volume mount in docker-compose.yml and in
docs/design-decisions.md — read that before treating this pattern as a
default choice rather than a deliberate one for this specific stack.

Known local-dev setup step, not automated here: the Airflow containers
need permission to talk to the mounted host Docker socket. On most Linux
hosts the socket is group-owned by `docker` with a host-specific GID, so a
fresh clone may need one of:
    sudo chmod 666 /var/run/docker.sock        # simplest, dev-only
or matching AIRFLOW_UID/GID to the host docker group's GID before build.
Not needed on Docker Desktop (macOS/Windows), where the socket is already
world-accessible inside the VM.

DAG structure:
    bronze.{orders, customers, products, fx_rates}      (parallel)
        -> silver.{products, customers, exchange_rates}  (parallel)
            -> silver.orders
                -> silver.order_items
                    -> dq_quality_gate  (logs quarantine %, never fails)
                        -> gold.{daily_sales, customer_360, monthly_revenue,
                                 inventory_summary, product_performance}  (parallel)
                            -> validate_trino_catalog

Gold tasks all wait on silver.order_items rather than their true minimal
per-table dependency (e.g. customer_360 only actually needs silver.orders
+ silver.customers, not order_items). Deliberate simplification: a single
join point after all of Silver is done is easier to reason about and
debug than five slightly-different dependency graphs, and at this data
volume the extra wait time is seconds, not minutes. Worth revisiting if
Gold ever needs to start before all of Silver finishes.

DQ-gate behavior (explicit decision, not a default): a Silver transform
that quarantines a meaningful share of its rows does NOT halt the DAG.
dq_quality_gate logs a WARNING with the exact quarantine percentage per
table and lets the DAG continue into Gold regardless. This is a
portfolio-appropriate choice — simpler to build, still demonstrates DQ
awareness via the quarantine tables and this log line. A production
version of this DAG would branch here: hard-fail (or page someone) above
a threshold instead of only logging. Said explicitly rather than silently
picked, since it's a real, debatable tradeoff.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.utils.task_group import TaskGroup

SPARK_SUBMIT = "docker exec master spark-submit /opt/spark-jobs/{path}"

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

# Silver table name -> quarantine table name are identical (see
# common/dq.py / silver_writer.py); listed here just for the DQ gate query.
SILVER_TABLES_WITH_QUARANTINE = [
    "products",
    "customers",
    "exchange_rates",
    "orders",
    "order_items",
]

DQ_WARN_THRESHOLD_PCT = 2.0  # matches the "more than a few percent" framing used to decide this


def _check_dq_quarantine(**context):
    """Logs quarantine % per Silver table via Trino. Never raises — see
    DQ-gate behavior in the module docstring for why this doesn't fail the
    task even when a table is well above threshold."""
    hook = TrinoHook(trino_conn_id="trino_default")
    log = context["ti"].log

    for table in SILVER_TABLES_WITH_QUARANTINE:
        try:
            silver_count = hook.get_first(f"SELECT count(*) FROM iceberg.silver.{table}")[0]
        except Exception:
            silver_count = 0

        try:
            quarantine_count = hook.get_first(
                f"SELECT count(*) FROM iceberg.quarantine.{table}"
            )[0]
        except Exception:
            # Quarantine table doesn't exist -> that Silver job quarantined
            # zero rows (write_quarantine_table is only called when there
            # are rows to write). Not an error condition.
            quarantine_count = 0

        total = silver_count + quarantine_count
        pct = round((quarantine_count / total) * 100, 2) if total > 0 else 0.0

        if pct > DQ_WARN_THRESHOLD_PCT:
            log.warning(
                "[dq_quality_gate] %s: %s/%s rows quarantined (%.2f%%) — "
                "above %s%% threshold, continuing anyway per DQ-gate policy "
                "(see DAG docstring)",
                table,
                quarantine_count,
                total,
                pct,
                DQ_WARN_THRESHOLD_PCT,
            )
        else:
            log.info(
                "[dq_quality_gate] %s: %s/%s rows quarantined (%.2f%%) — within threshold",
                table,
                quarantine_count,
                total,
                pct,
            )


def _validate_trino_catalog(**context):
    """Post-Gold sanity check: every Gold table should have rows and be
    reachable through Trino, since Trino is the query layer this whole
    project is meant to demonstrate. Raises (fails the task) if a Gold
    table is missing or empty — unlike the DQ gate, an empty Gold table
    means the pipeline produced nothing usable, which should be visible
    as a DAG failure, not a warning."""
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
    default_args={"retries": 1},
    tags=["lakehouse", "medallion"],
) as dag:

    with TaskGroup("bronze") as bronze_group:
        bronze_tasks = {
            name: BashOperator(
                task_id=f"extract_{name}",
                bash_command=SPARK_SUBMIT.format(path=path),
            )
            for name, path in BRONZE_JOBS
        }

    with TaskGroup("silver") as silver_group:
        silver_independent_tasks = {
            name: BashOperator(
                task_id=f"transform_{name}",
                bash_command=SPARK_SUBMIT.format(path=path),
            )
            for name, path in SILVER_INDEPENDENT_JOBS
        }

        silver_orders_task = BashOperator(
            task_id="transform_orders",
            bash_command=SPARK_SUBMIT.format(path="silver/transform_orders.py"),
        )
        silver_order_items_task = BashOperator(
            task_id="transform_order_items",
            bash_command=SPARK_SUBMIT.format(path="silver/transform_order_items.py"),
        )

        # orders needs silver.customers (FK check) + silver.exchange_rates (fx conversion)
        [silver_independent_tasks["customers"], silver_independent_tasks["exchange_rates"]] >> silver_orders_task
        # order_items needs silver.orders (FK + inherited order_date) + silver.products (FK)
        [silver_orders_task, silver_independent_tasks["products"]] >> silver_order_items_task

    dq_quality_gate = PythonOperator(
        task_id="dq_quality_gate",
        python_callable=_check_dq_quarantine,
    )

    with TaskGroup("gold") as gold_group:
        gold_tasks = [
            BashOperator(
                task_id=f"compute_{name}",
                bash_command=SPARK_SUBMIT.format(path=path),
            )
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
