# Base Airflow image + the Spark provider, so DAGs can use SparkSubmitOperator
# to submit jobs to spark-master over the shared docker network, and the
# Trino provider so DAGs can run the post-load validation queries described
# in docs/design-decisions.md (Airflow DAG stage: "Validate Trino Queries").
FROM apache/airflow:2.9.3-python3.11

USER root
# openjdk headless: SparkSubmitOperator shells out to spark-submit, which
# needs a local JVM even though the actual job runs on the remote cluster.
# docker.io: gives this container the `docker` CLI so DAG tasks can run
# `docker exec master spark-submit ...` against the host's Docker socket
# (mounted in docker-compose.yml) — see the socket-mount comment there for
# why this exists instead of a full local Spark client. Only the CLI
# binary is actually used; the bundled dockerd is never started here.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless docker.io \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
      apache-airflow-providers-apache-spark==4.11.0 \
      apache-airflow-providers-trino==5.7.1 \
      great-expectations==0.18.19
