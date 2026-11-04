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
    - ui=False for tests: the web UI (http://localhost:4040 while a job runs) is not needed there.
    - Python workers use the same interpreter as the driver (the active venv). Without this, Spark starts
      whatever `python` is first in PATH, which on Windows often fails with "Python worker failed to connect back".
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", driver_memory))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", str(ui).lower())
        .getOrCreate()
    )
