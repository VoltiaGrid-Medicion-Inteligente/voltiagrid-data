"""Daily readings pipeline (US-39): raw -> clean -> filled, with dependencies and retries (T-39.1) and parameters
(T-39.2).

Every day at 01:00 UTC it processes the F3 readings that reached raw the day before (ingest_date = the run's date).
Each task is one job of this repo. With VOLTIA_RUNTIME=ecs the Spark jobs run as ECS Fargate tasks on the S3 lake and
the pull jobs (F4 export, F5 fetch) in the worker (US-40, T-40.3, voltiagrid_common.py); by default everything runs in
the worker with `python -m jobs.<job>` on the local lake.

Parameters (Trigger DAG w/ config, or `airflow dags trigger voltiagrid_readings -c '{"date": "2026-11-02"}'`):
    date    raw partition (ingest date) to process; empty = the run's date. Only that period is processed.
    source  f3 (concentrators, default) or f1 (Kaggle history loaded to raw in US-23)
    mode    dev (the 4 blocks of the development subset) or full (all 112 blocks); only used with source f1

    clean_readings   --> estimate_gaps, control_counts, fetch_f5
    export_inventory --> circuit_load, fact_consumption, transformer_losses
    estimate_gaps    --> circuit_load, fact_consumption, transformer_losses
    fact_consumption --> billing
    fetch_f5         --> transformer_losses --> suspicious_meters
    control_counts fails if a reading was lost or duplicated between raw and clean (CA-08).

Owners of each task (T-39.4): clean_readings, estimate_gaps, control_counts: data engineering (US-30, US-34, US-31);
export_inventory, circuit_load: US-33; fact_consumption, billing: US-35, US-36; fetch_f5, transformer_losses,
suspicious_meters: US-37, US-38.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from voltiagrid_common import spark_job, worker_job

INGEST_DATE = "{{ params.date or ds }}"
# name of the cleaning run manifest (clean/.../_runs/<run>.json), as jobs.clean_readings.run_id builds it
RUN = "{{ ('f3-' if params.source == 'f3' else 'f1-' ~ params.mode ~ '-') ~ (params.date or ds) }}"

default_args = {
    "owner": "voltiagrid",
    "retries": 2,                          # a failed task is tried 2 more times...
    "retry_delay": timedelta(minutes=2),   # ...after 2 minutes, then 4 (exponential backoff)
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=40),  # inside the 45 minutes of the daily close (US-55)
}


with DAG(
    dag_id="voltiagrid_readings",
    description="Daily: clean the readings of the day before, estimate gaps, then load, billing and losses",
    start_date=datetime(2026, 10, 1),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,            # two runs at once would overwrite the same partitions
    default_args=default_args,
    params={
        "date": Param(None, type=["null", "string"], format="date",
                      description="Ingest date of the raw partition to process (YYYY-MM-DD). Empty: the run's date."),
        "source": Param("f3", enum=["f3", "f1"], description="f3 = concentrators, f1 = Kaggle history"),
        "mode": Param("dev", enum=["dev", "full"], description="F1 only: dev = 4 dev blocks, full = 112 blocks"),
    },
    tags=["voltiagrid", "pipeline"],
) as dag:
    clean = spark_job("clean_readings",
                      f"--source {{{{ params.source }}}} --ingest-date {INGEST_DATE} --mode {{{{ params.mode }}}}")
    estimate = spark_job("estimate_gaps", f"--run {RUN}")
    control = spark_job("control_counts", f"--ingest-date {INGEST_DATE}", condition="params.source == 'f3'",
                        otherwise="control counts compare F3 raw with clean; nothing to check for F1")

    inventory = worker_job("export_inventory", "python -m jobs.inventory export")
    circuits = spark_job("circuit_load", f"--run {RUN}")
    consumption = spark_job("fact_consumption", f"--run {RUN}")
    bills = spark_job("billing", f"--run {RUN}")
    f5 = worker_job("fetch_f5", f"python -m jobs.fetch_f5 --run {RUN}")
    losses = spark_job("transformer_losses", f"--run {RUN}")
    meters = spark_job("suspicious_meters", f"--run {RUN}")

    clean >> [estimate, control, f5]
    [inventory, estimate] >> circuits
    [inventory, estimate] >> consumption >> bills
    [inventory, estimate, f5] >> losses >> meters
