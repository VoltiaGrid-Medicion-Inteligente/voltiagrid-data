"""Daily readings pipeline (US-39): raw -> clean -> filled, with dependencies and retries (T-39.1).

Every day at 01:00 UTC it processes the F3 readings that reached raw the day before (ingest_date = the run's date).
Each task is one Spark job of this repo, run in the Airflow worker with `python -m jobs.<job>`.

    clean_readings --> estimate_gaps
                  \\-> control_counts      (fails if a reading was lost or duplicated between raw and clean)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

INGEST_DATE = "{{ ds }}"
RUN = "f3-{{ ds }}"  # name of the cleaning run manifest (clean/.../_runs/<run>.json), read by the next jobs
LAKE = "--lake $VOLTIA_LAKE"

default_args = {
    "owner": "voltiagrid",
    "retries": 2,                          # a failed task is tried 2 more times...
    "retry_delay": timedelta(minutes=2),   # ...after 2 minutes, then 4 (exponential backoff)
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=40),  # inside the 45 minutes of the daily close (US-55)
}


def job(task_id: str, args: str) -> BashOperator:
    """One Spark job of this repo. The command runs from the repo folder, so relative paths like
    data/reference/tariffs.csv work the same as on a laptop."""
    return BashOperator(task_id=task_id, bash_command=f"cd $VOLTIA_REPO && python -m jobs.{task_id} {args} {LAKE}")


with DAG(
    dag_id="voltiagrid_readings",
    description="Daily: clean the readings of the day before, estimate gaps, check control counts",
    start_date=datetime(2026, 10, 1),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,            # two runs at once would overwrite the same partitions
    default_args=default_args,
    tags=["voltiagrid", "pipeline"],
) as dag:
    clean = job("clean_readings", f"--source f3 --ingest-date {INGEST_DATE}")
    estimate = job("estimate_gaps", f"--run {RUN}")
    control = job("control_counts", f"--ingest-date {INGEST_DATE}")

    clean >> [estimate, control]
