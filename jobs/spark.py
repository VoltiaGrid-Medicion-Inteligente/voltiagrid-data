"""One way to start Spark for every job and test, so they all behave the same."""

import os
import sys

from pyspark.sql import SparkSession


def get_spark(app_name: str, master: str = "local[*]", driver_memory: str = "2g",
              shuffle_partitions: int = 8, ui: bool = True) -> SparkSession:
    """Spark in local mode (also on Fargate, ADR-014): one process with every core of the machine.

    - Session time zone UTC: timestamps in the lake are UTC wall-clock times (Data lake layout, rule 2).
    - Dynamic partition overwrite: a job rewrites only the date partitions it produced (rule 5).
    - SPARK_DRIVER_MEMORY, when set, replaces driver_memory (the Airflow workers set it so parallel tasks fit in RAM).
    - SPARK_SHUFFLE_PARTITIONS, when set, replaces shuffle_partitions (US-42): 8 suits a laptop or a 2-vCPU task; a
      big task (the F1 history on 8 vCPU) wants about 4 per core. Adaptive execution merges the small ones.
    - ui=False for tests: the web UI (http://localhost:4040 while a job runs) is not needed there.
    - Python workers use the same interpreter as the driver (the active venv). Without this, Spark starts
      whatever `python` is first in PATH, which on Windows often fails with "Python worker failed to connect back".
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", driver_memory))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", str(shuffle_partitions)))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", str(ui).lower())
    )
    for key, value in s3a_options().items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def s3a_options() -> dict[str, str]:
    """S3A settings from the environment, for the jobs that read and write s3a:// paths (US-40, T-40.1).

    - Credentials: none here. S3A's default chain finds them: the ECS task role on Fargate, AWS_* variables or an
      AWS profile on a laptop. Nothing secret goes into the Spark configuration.
    - AWS_REGION (set in the ECS task definition): the region of the lake buckets.
    - S3_ENDPOINT: only for an S3-compatible server such as MinIO (the CI image check); path-style URLs and http.
    The connector classes (hadoop-aws) are in the Spark jobs image (docker/spark/Dockerfile); a laptop running only
    local paths does not need them.
    """
    options = {}
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    if region:
        options["spark.hadoop.fs.s3a.endpoint.region"] = region
    endpoint = os.getenv("S3_ENDPOINT")
    if endpoint:
        options["spark.hadoop.fs.s3a.endpoint"] = endpoint
        options["spark.hadoop.fs.s3a.path.style.access"] = "true"
        options["spark.hadoop.fs.s3a.connection.ssl.enabled"] = str(endpoint.startswith("https://")).lower()
    return options
