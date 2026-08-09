# Design Decisions — Infrastructure & Pipeline Layer

Each section: the decision, why, the alternative(s) considered, and a
common mistake to avoid. Sections 1–7 cover what `docker-compose.yml`
commits us to; sections 8–9 cover the Airflow DAG and Spark job design
choices layered on top of it.

---

## 1. Iceberg as the table format, Hive Metastore as the catalog

**Decision:** Every Bronze/Silver/Gold table is an Iceberg table. Hive
Metastore stores only *metadata* (table locations, schemas, partition
specs) — it never owns the actual data files.

**Why:** Iceberg gives us the things Hive tables cannot do natively:
hidden partitioning (partition columns don't have to be part of every
query predicate), safe concurrent writes with snapshot isolation, and —
critically for a data-quality-focused project — the ability to
`MERGE INTO`, `UPDATE`, and `DELETE` at the row level instead of
rewriting whole partitions. Hive tables are append/overwrite-partition
only; retrofitting late-arriving correction rows into a Hive table means
rewriting the partition file.

**Why Hive Metastore specifically, and not Iceberg's REST catalog or AWS
Glue:** the project spec calls for legacy SQL compatibility and a
metadata layer multiple engines (Spark, Hive, Trino) can share without
each maintaining its own view of "what tables exist." Hive Metastore is
the lowest-common-denominator catalog every one of those engines already
understands out of the box.

**Alternative considered:** Delta Lake. Very similar feature set. Iceberg
was chosen because its catalog is engine-agnostic (Spark, Trino, Flink,
Hive all read/write the same table without a proprietary connector),
which matters specifically because this project's whole premise is
multi-engine access (Spark writes, Trino and Hive read).

**Common mistake:** treating Hive as "where the data lives." It isn't —
`hive.metastore.warehouse.dir` in `hive-site.xml` is the location for
*Hive-managed* tables, which this project doesn't use. Iceberg tables
live under a separate warehouse path (`ICEBERG_WAREHOUSE`) and Hive only
stores a pointer to it.

---

## 2. HDFS instead of S3/MinIO

**Decision:** Storage is HDFS (NameNode + DataNode), not object storage.

**Why:** This is a project-spec requirement, and it is a defensible one
to keep: a large share of enterprise data platforms — especially in
banking, telecom, and government — still run Hadoop on-prem rather than
in the cloud, often for data-residency or existing capex reasons.
Demonstrating HDFS literacy (block replication, NameNode/DataNode roles,
the RPC vs. web UI ports) is still a relevant, hireable skill in those
markets.

**Caveat:** Most modern lakehouse builds (2026) use S3/ADLS/GCS with
Iceberg. Iceberg supports multiple filesystems; the same table DDL works
against `s3a://` or `hdfs://`. HDFS was selected deliberately for this
project's scenario.

**Common mistake:** setting `dfs.replication` to 1 without explaining the
reason. The single-DataNode Docker Compose stack cannot replicate to
three nodes; production deployments should use a higher replication
factor.

---

## 3. Spark Standalone, not YARN or Kubernetes

**Decision:** `spark-master` + `spark-worker` containers running Spark's
own built-in cluster manager, not Hadoop YARN (despite YARN being
available since we are already running Hadoop) and not Kubernetes.

**Why:** YARN adds ResourceManager + NodeManager containers and a config
surface (queues, container sizing, `yarn-site.xml`) that buys nothing at
this scale — a two-node Spark cluster on one laptop. Standalone mode is
Spark's own scheduler, needs zero extra services, and demonstrates the
same core Spark concepts (driver/executor, DAG scheduling, shuffle) the
job actually depends on.

**Alternative considered:** Kubernetes (`spark-on-k8s`). More
representative of where the industry is heading, but adds a K8s cluster
as a prerequisite just to run a portfolio project — disproportionate
setup cost for the audience (reviewers who expect `docker compose up -d`
to start the stack).

**Common mistake:** standing up YARN or Kubernetes unnecessarily for
small lab-scale demos; Standalone is simpler and clearer for this
project's scope.

---

## 4. Trino as the virtualization layer, not just another query engine

**Decision:** Trino gets three catalogs: `iceberg` (bronze/silver/gold),
`postgresql` (live orders), `mysql` (live customers). A single Trino
query can join across all three without any of them being copied into
the others.

**Why data virtualization at all, rather than just ETL-ing everything
into Iceberg:** ETL costs freshness (data is as stale as the last run)
and duplicates storage/governance surface. Virtualization costs query
performance (queries are bound by the source system's ability to serve
the pushed-down predicate) and adds load to operational databases. This
project deliberately keeps *operational* data (orders, customers)
queryable live via Trino, while *analytical* aggregates (Gold) go through
the ETL path — that split is the actual design decision, not
"virtualize everything" or "ETL everything."

**Rule of thumb:** move data (ETL) when it needs to be aggregated,
reshaped, or joined with historical/analytical data at scale, or when
the source system cannot absorb analytical query load. Query in place
(virtualize) when freshness matters more than transformation complexity,
and the source can handle the query volume.

**Common mistake:** pointing Trino's `postgresql`/`mysql` catalogs at the
same credentials an application uses for writes. A dedicated
read-oriented user (or read replica, in production) keeps analytical
query load from competing with — or accidentally mutating — operational
traffic. The `.env.example` credentials here are separate per catalog for
exactly this reason, even though in this portfolio project they happen to
have the same privileges.

**Operational note:** the catalog properties files reference their
connection details as `${ENV:VAR_NAME}`, not plaintext — which means the
`trino` service's `environment:` block in `docker-compose.yml` must
explicitly pass through `POSTGRES_ORDERS_DB`/`POSTGRES_ORDERS_USER`/
`POSTGRES_ORDERS_PASSWORD` (and the MySQL equivalents). This isn't
inherited automatically just because those variables exist in `.env` —
Trino only sees what the Compose service definition forwards to it. Easy
to miss when adding a new catalog or renaming a variable, since the
failure mode is Trino refusing to start rather than a clear "missing
env var" message.

---

## 5. Hive Metastore's backing database is its own Postgres instance

**Decision:** `hive-metastore-db` is a dedicated Postgres container. It
is **not** the same Postgres instance as `postgres-orders`.

**Why:** two unrelated reasons to keep these separate, both worth having
ready:
1. **Blast radius / coupling** — the metastore schema is Hive's internal
   implementation detail (`TBLS`, `SDS`, `PARTITIONS`, etc.). Sharing an
   instance with an operational app database means a metastore migration,
   backup restore, or resource-exhaustion incident can take down order
   processing, and vice versa.
2. **Workload isolation** — metastore traffic is small but
   latency-sensitive (every Spark/Trino/Hive query touches it for
   planning); operational traffic is a different, unrelated load
   profile. Co-locating them means one can starve the other under load.

**Common mistake:** reusing one Postgres container for "everything
Postgres" to save a container. It is tempting and it does technically
work — but it erases a distinction ("would you separate these in
production, and why") that is relevant to production architecture.

---

## 6. Airflow: `LocalExecutor`, one Postgres, no Redis/Celery

**Decision:** Airflow runs scheduler, webserver, and triggerer against a
single dedicated Postgres, using `LocalExecutor` (tasks run as
subprocesses on the scheduler host) rather than `CeleryExecutor` or
`KubernetesExecutor`.

**Why:** this pipeline's task count and concurrency needs (a handful of
Bronze/Silver/Gold tasks per DAG run, once a day) don't need distributed
task execution. `CeleryExecutor` adds a message broker (Redis/RabbitMQ)
and worker containers whose only job here would be running the exact
same `SparkSubmitOperator` calls a local subprocess can make just as
well, since the actual heavy lifting already happens on the separate
Spark cluster — Airflow's executor only decides *where the lightweight
`spark-submit` call itself runs*, not where the job executes.

**Common mistake:** conflating Airflow's executor choice with where heavy
work runs — `spark-submit` executes on the Spark cluster regardless of
the executor.

---

## 7. Adopting existing prebuilt Hadoop/Spark/Hive images instead of building fresh containers

**Decision:** The Hadoop/Spark/Hive corner of this stack runs on
`hadoop-hive-spark-master` / `-worker` / `-history` — images already
built locally for an earlier, unrelated project — instead of separate
`bde2020/hadoop-*` + `bitnami/spark` + `apache/hive` containers.

**Why:** avoiding 10+ GB of redundant image pulls on a machine that
already had a working Hadoop/Spark/Hive stack sitting in its Docker
cache. Bandwidth and disk aside, this is also a genuinely realistic
scenario worth having a story for: joining a team almost never means
building infrastructure from a blank page — it usually means inheriting
preexisting containers, sparse documentation, and hardcoded assumptions,
requiring reverse-engineering to understand configuration before making
changes.

**How the wiring was actually determined:** none of it was guessed. The
images weren't built via `docker compose`, so there was no compose file
to read — everything below came from `docker inspect` (env vars, exposed
ports) and `docker run --rm <image> cat <path>` against the actual
`entrypoint.sh` / `run.sh` / `hive-site.xml` / `core-site.xml` /
`hdfs-site.xml` inside each image. That process surfaced several
non-obvious, non-configurable constraints:

| What | Value | Where it is fixed |
|---|---|---|
| HDFS/Spark master hostname | `master` | `fs.defaultFS=hdfs://master` in `core-site.xml`; `spark://master:7077` hardcoded as a literal string in `worker`'s `run.sh` |
| Hive Metastore location | runs *inside* the `master` container | `master`'s `run.sh` starts `hive --service metastore` and `hiveserver2` as background processes, after formatting HDFS and before starting Spark Master in the foreground |
| Metastore DB connection | `jdbc:postgresql://metastore:5432/metastore`, user/pass `jupyter`/`jupyter` | baked into `hive-site.xml`'s `javax.jdo.option.*` properties |
| Metastore schema init | automatic, once | `run.sh` runs `schematool -dbType postgres -initSchema` on first boot only, guarded by a marker file in the NameNode data dir |
| `SPARK_MASTER_HOST` | must be supplied at container start | referenced in `run.sh` but absent from the image's baked-in env — the image expects it to be passed via `docker run -e` or a compose `environment:` block |
| HDFS replication factor | baked in as `3` | `hdfs-site.xml` — overridden to `1` via a bind-mounted replacement, since this topology only ever runs one DataNode (`worker`) |
| Spark default cluster manager | `yarn` (in `spark-defaults.conf`) | but `worker` explicitly joins `spark://master:7077` as a Standalone worker regardless — both YARN daemons and Standalone Master/Worker run side by side. Jobs are submitted with `--master spark://master:7077` explicitly rather than relying on the baked-in default, to ensure the Standalone cluster is used. |
| No Iceberg runtime, no MySQL driver | — | neither jar is in `$SPARK_HOME/jars`; added by bind-mounting two files (not rebuilding the image) — see `docker/spark-jars/README.md` |

**Common mistake:** assuming a convenience image like this one (HDFS,
YARN, Hive Metastore, and Spark Master all in a single container)
reflects how this would be architected in production. It doesn't — this
is a lab-scale shortcut, not a production pattern, and the README's
"not production" section says so explicitly.

---

## 8. Airflow submits Spark jobs through `SparkSubmitOperator` against the standalone cluster

**Decision:** every Bronze/Silver/Gold task is a `SparkSubmitOperator`
that submits to the existing Spark standalone cluster at
`spark://master:7077` via the Airflow connection `spark_default`.

**Why:** the repo already has a dedicated Spark master/worker cluster, and
Airflow no longer needs Docker daemon access to reach it. The job logic is
unchanged; only the orchestration mechanism is different.

**Trade-off:** this keeps the stack aligned with a normal Airflow + Spark
pattern while preserving the existing Compose topology and job paths.

**Gotcha:** setting `AIRFLOW_CONN_SPARK_DEFAULT` to a bare
`spark://master:7077` is not safe to assume works as-is — Airflow's Spark
provider can strip or mishandle the `spark://` scheme depending on exactly
how the connection URI/extras are structured, and when that happens the
failure is silent: the task doesn't error, it just doesn't submit to the
standalone cluster the way you'd expect. If this connection is ever
reconfigured, verify the resulting `--master` value the operator actually
submits with (e.g. via the task log's rendered spark-submit command)
rather than trusting the connection string alone.

---

## 9. Incremental extraction and its limits

**Decision:** Bronze extraction for `orders`, `order_items`, `customers`,
and `exchange_rates` is watermark-based (see
`spark/jobs/common/incremental.py`): each run only reads source rows past
a stored watermark and appends them to Bronze, instead of re-reading and
overwriting the entire source table every run. `products` stays
full-refresh — see the docstring in `bronze/extract_products.py` for why
(no usable watermark column, and cheap to fully re-read at ~2,000 rows).

**What the watermark is, per table:**

| Table | Watermark column | What it is |
|---|---|---|
| `orders` | `order_ts` | the row's own creation timestamp |
| `order_items` | `order_item_id` | monotonic surrogate PK (no timestamp column exists on this table) |
| `customers` | `customer_id` | monotonic surrogate PK (same reason) |
| `exchange_rates` | `rate_date` | the row's own business date |

**What this does NOT do — and why that's a real limitation, not a rounding
error:** none of these source tables expose an `updated_at`/`modified_at`
column. A watermark comparison (`WHERE col > last_watermark`) can only
ever find rows that are *new* relative to what's already been extracted —
it structurally cannot distinguish "this row didn't exist before" from
"this row existed before and was edited in place," because there is
nothing in the row to compare against a prior version. Concretely: if an
order's `order_status` changes from `PLACED` to `SHIPPED` after that order
has already been extracted into Bronze, this pipeline will never see that
change. The stale `PLACED` row sits in Bronze (and downstream Silver/Gold)
indefinitely.

**What closing that gap actually requires** (neither is a Bronze-job
change — both are source-system changes):
1. **An `updated_at` column**, maintained by every write path that can
   mutate a row (the application, any admin tooling, migrations) — and
   then watermarking on `updated_at` instead of the creation timestamp.
   This only works if that column is *reliably* set on every mutation;
   one un-instrumented write path silently reintroduces the same gap.
2. **CDC** (e.g. Debezium reading Postgres's WAL / MySQL's binlog) —
   captures every row-level change, including updates and deletes, by
   reading the database's own replication stream rather than querying
   application tables at all. This is the production-grade answer, and
   the one that scales past "which columns happen to exist."

**Common mistake:** describing watermark-based extraction as "CDC" or
"catches all changes." It doesn't — it's insert-only-aware incremental
extraction, a real and useful improvement over full-refresh, but not a
substitute for genuine change-data-capture when source rows are mutated
in place.

**Related trade-off — Silver has no history between runs:**
`silver_writer.py`'s write path is `DROP TABLE IF EXISTS` followed by a
fresh `create()`, for both the main Silver tables and the quarantine
tables. So even though Bronze accumulates incrementally, each Silver run
fully rebuilds its output from whatever is in Bronze at that moment —
there's no append-only history at the Silver layer, and a bad run
overwrites the previous good one with no built-in rollback. This is a
deliberate simplicity choice for a portfolio project (no SCD/versioning
logic to maintain), but it's a real design trade-off worth being able to
name: a production system would likely want Silver to be append-only or
merge-based (Iceberg's `MERGE INTO`) so a bad run doesn't destroy the last
known-good state.

---