# Base Airflow image + the Spark provider, so DAGs can use SparkSubmitOperator
# to submit jobs to spark-master over the shared docker network, and the
# Trino provider so DAGs can run the post-load validation queries described
# in docs/design-decisions.md (Airflow DAG stage: "Validate Trino Queries").
FROM apache/airflow:2.9.3-python3.11

USER root
# openjdk headless: SparkSubmitOperator shells out to spark-submit, which
# needs a local JVM even though the actual job runs on the remote cluster.
# The local Spark client is installed in /opt/spark so the operator can
# submit directly to spark://master:7077 without Docker socket access.
ENV SPARK_HOME=/opt/spark
ENV PATH=${SPARK_HOME}/bin:${PATH}
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl openjdk-17-jre-headless \
 && curl -fsSL https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz -o /tmp/spark.tgz \
 && tar -xzf /tmp/spark.tgz -C /opt \
 && mv /opt/spark-3.3.1-bin-hadoop3 ${SPARK_HOME} \
 && rm -f /tmp/spark.tgz \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

USER airflow
# Not installing great-expectations here: nothing in this repo imports it.
# DQ validation is deliberately handled the simpler way instead — Silver
# writes flag/quarantine bad rows directly (see common/dq.py), and
# dq_quality_gate in the DAG queries those quarantine tables via Trino.
# A real GE integration would be a legitimate next step to point to in an
# interview, but installing the dependency without using it just bloats
# the image and build time for nothing.
RUN pip install --no-cache-dir \
      apache-airflow-providers-apache-spark==4.11.0 \
      apache-airflow-providers-trino==5.7.1