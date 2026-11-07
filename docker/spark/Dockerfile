# Spark jobs image (US-40, T-40.1): Python 3.12, Java 17, PySpark and the S3A connector, so any job of this repo runs
# as an ECS Fargate task that reads and writes the S3 lake (ADR-002, ADR-014: Spark in local mode in one container).
# Built from the repo root:
#   docker build -f docker/spark/Dockerfile -t voltiagrid/spark-jobs:local .
#   docker run --rm voltiagrid/spark-jobs:local python -m jobs.clean_readings --help
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/voltiagrid

# Same versions as on the laptops and in the CI (requirements.txt).
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# S3A connector: hadoop-aws of the same Hadoop version that PySpark ships, plus the AWS SDK it needs, copied into
# PySpark's jars folder so every job finds them without downloading anything at run time.
COPY docker/spark/fetch_s3a_jars.py /tmp/fetch_s3a_jars.py
RUN python /tmp/fetch_s3a_jars.py && rm -rf /tmp/ivy /root/.ivy2 /tmp/fetch_s3a_jars.py

WORKDIR /opt/voltiagrid
COPY jobs ./jobs
COPY pipeline ./pipeline
COPY schemas ./schemas
COPY data/reference ./data/reference
COPY dev_subset.py ./
COPY docker/spark/check_image.py ./docker/spark/check_image.py

# Not root: the jobs only need /tmp (Spark's scratch space) and the network.
RUN useradd --uid 10001 --create-home spark
USER spark

# ECS overrides the command with the job and its arguments.
CMD ["python", "docker/spark/check_image.py"]
