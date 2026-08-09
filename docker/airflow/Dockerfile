# Airflow image with Spark + Trino providers for SparkSubmitOperator.
FROM apache/airflow:2.9.3-python3.11

USER root
# SparkSubmitOperator needs a local JVM and a Spark client for submission.
# Keep the runtime in /opt/spark so the DAG can target spark://master:7077.
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
# Keep the image lean: DAG validation uses Trino directly, not Great Expectations.
RUN pip install --no-cache-dir \
      apache-airflow-providers-apache-spark==4.11.0 \
      apache-airflow-providers-trino==5.7.1