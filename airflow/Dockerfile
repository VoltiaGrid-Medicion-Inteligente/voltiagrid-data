# Airflow image for VoltiaGrid (US-32, T-32.1): Airflow 2.10 + Java 17 + PySpark, so a task can run the Spark jobs of
# this repo in local mode (ADR-002, ADR-014). Built by airflow/docker-compose.yml; by hand, from the repo root:
#   docker build -f airflow/Dockerfile -t voltiagrid/airflow:local .
FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17 \
    PYTHONDONTWRITEBYTECODE=1

USER airflow
# Same versions as requirements.txt. Airflow is pinned in the same command so pip never upgrades or downgrades it.
RUN pip install --no-cache-dir "apache-airflow==2.10.5" "pyspark==4.2.0" "py4j==0.10.9.9" "boto3>=1.35" "psycopg[binary]>=3.2"
