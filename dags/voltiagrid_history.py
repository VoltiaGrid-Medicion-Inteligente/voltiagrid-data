"""One-off load of the F1 history up to curated (US-41, T-41.1; CA-09).

Started by hand (no schedule) once F1 is in raw (US-23). The same jobs as the daily DAG, in the order that takes the
history from raw to curated:

    clean_readings (F1) --> estimate_gaps --> fact_consumption
    export_inventory    -----------------/

With VOLTIA_RUNTIME=ecs the Spark jobs run on Fargate **Spot** (ADR-014: if Spot takes the task back, Airflow retries
it from scratch; the daily DAG stays on on-demand) and on a bigger task than the daily jobs: 8 vCPU, 32 GB and
100 GiB of scratch disk, because the full history is about 167 million readings in one run. Locally (default) the
jobs run in the workers; use mode=dev there, the full history does not fit a laptop.

Parameters:
    ingest_date  raw partition where F1 was uploaded (US-23), e.g. 2026-10-28. Required.
    mode         full (all 112 blocks, default) or dev (the 4 blocks of the development subset)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from voltiagrid_common import run_cost_task, spark_job, worker_job

RUN = "f1-{{ params.mode }}-{{ params.ingest_date }}"   # same name as jobs.clean_readings.run_id
HISTORY_TASK = {"capacity": "FARGATE_SPOT", "cpu": 8192, "memory": 32768, "storage_gib": 100}
# estimate_gaps and fact_consumption run on on-demand: a Spot interruption in the middle of them lost 2 h 54 min of work
# (the run of 2026-11-08, exit 137), and a retry starts from scratch. Same size, no interruptions.
ON_DEMAND_TASK = {**HISTORY_TASK, "capacity": "FARGATE"}
LONG_TIMEOUT = timedelta(hours=8)

default_args = {
    "owner": "voltiagrid",
    "retries": 2,                          # also covers a Spot interruption: the job starts again from scratch
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(hours=4),
}

with DAG(
    dag_id="voltiagrid_history",
    description="One-off: the F1 history from raw to curated (clean, estimate, monthly consumption)",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "ingest_date": Param(type="string", format="date",
                             description="Raw partition with the F1 upload (YYYY-MM-DD), e.g. 2026-10-28"),
        "mode": Param("full", enum=["full", "dev"], description="full = 112 blocks; dev = the 4 dev blocks"),
    },
    tags=["voltiagrid", "history"],
) as dag:
    clean = spark_job("clean_readings", "--source f1 --ingest-date {{ params.ingest_date }} --mode {{ params.mode }}",
                      **HISTORY_TASK)
    estimate = spark_job("estimate_gaps", f"--run {RUN}", **ON_DEMAND_TASK)
    inventory = worker_job("export_inventory", "python -m jobs.inventory export")
    consumption = spark_job("fact_consumption", f"--run {RUN}", **ON_DEMAND_TASK)
    for long_task in (estimate, consumption):   # estimate_gaps did not finish in 2 h 54 min: give both 8 hours
        long_task.execution_timeout = LONG_TIMEOUT
        if hasattr(long_task, "waiter_max_attempts"):            # ECS: keep polling as long as the timeout allows
            long_task.waiter_max_attempts = int(LONG_TIMEOUT.total_seconds() // 15)

    cost = run_cost_task()      # time, resources and cost of the run (T-41.2, CA-09)

    clean >> estimate
    [estimate, inventory] >> consumption
    [clean, estimate, inventory, consumption] >> cost
