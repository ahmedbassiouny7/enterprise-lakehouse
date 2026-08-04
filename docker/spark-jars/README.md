# Extra Spark jars

Your `hadoop-hive-spark-*` images ship Spark 3.3.1 with the Postgres JDBC
driver already in `$SPARK_HOME/jars` — but no Iceberg runtime and no MySQL
driver. Rather than rebuild your images (which would re-trigger a big
layer download), the two missing jars are fetched once to your host here
and bind-mounted straight into `/opt/spark/jars/` on `master`, `worker`,
and `history` in `docker-compose.yml`. Spark loads everything in that
directory onto the classpath automatically — no `--jars` flag needed on
every `spark-submit`.

Run this once, from the repo root, before your first `docker compose up`:

```bash
mkdir -p docker/spark-jars
curl -fLo docker/spark-jars/iceberg-spark-runtime-3.3_2.12-1.4.3.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.4.3/iceberg-spark-runtime-3.3_2.12-1.4.3.jar

curl -fLo docker/spark-jars/mysql-connector-j-8.4.0.jar \
  https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar
```

**Why 1.4.3 specifically:** it's the last `iceberg-spark-runtime-3.3_2.12`
release Apache Iceberg published (Dec 2023) — matches your image's Spark
3.3.1 / Scala 2.12 exactly. Don't substitute a newer Iceberg version here;
newer runtime jars stop publishing 3.3-targeted builds.

These two files are gitignored (see `.gitignore`) — don't commit binaries,
just the download command.

**Download them before your first `docker compose up`, not after.**
`docker-compose.yml` bind-mounts each jar as an individual file (not this
whole folder) directly into `/opt/spark/jars/` on `master`/`worker`/
`history`. If the file doesn't exist on the host yet when Compose creates
the mount, Docker silently creates an empty *directory* at that path inside
the container instead of erroring — Spark then either skips it or fails
confusingly, and it looks nothing like a "file not found" error. If Spark
jobs later can't find Iceberg classes, check `docker exec master ls -la
/opt/spark/jars/iceberg-spark-runtime-3.3_2.12-1.4.3.jar` first — if it's a
directory, this is why.

## Every Spark job needs these two `--conf` flags

The image's baked-in `spark-defaults.conf` sets `spark.master yarn` and has
no Iceberg catalog configured — both are fine for this stack's original use
case, but wrong for us. Rather than edit the baked-in file (risking breaking
something else in an image you didn't build), always pass this explicitly
on every `spark-submit` / `SparkSubmitOperator` call in this project:

```
--master spark://master:7077 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.lakehouse.type=hive \
--conf spark.sql.catalog.lakehouse.uri=thrift://master:9083 \
--conf spark.sql.catalog.lakehouse.warehouse=hdfs://master/warehouse/iceberg
```

This will get wrapped into a shared helper once we build the Airflow DAGs
(Phase 4) so it isn't copy-pasted into every task — noting it here for now
since it's needed the moment you run anything interactively too.
