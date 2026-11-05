"""Daily readings pipeline (US-39): raw -> clean -> filled, with dependencies and retries (T-39.1) and parameters
(T-39.2).

Every day at 01:00 UTC it processes the F3 readings that reached raw the day before (ingest_date = the run's date).
Each task is one Spark job of this repo, run in the Airflow worker with `python -m jobs.<job>`.

Parameters (Trigger DAG w/ config, or `airflow dags trigger voltiagrid_readings -c '{"date": "2026-11-02"}'`):
    date    raw partition (ingest date) to process; empty = the run's date. Only that period is processed.
    source  f3 (concentrators, default) or f1 (Kaggle history loaded to raw in US-23)
    mode    dev (the 4 blocks of the development subset) or full (all 112 blocks); only used with source f1

    clean_readings --> estimate_gaps
                  \\-> control_counts      (fails if a reading was lost or duplicated between raw and clean)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

INGEST_DATE = "{{ params.date or ds }}"
# name of the cleaning run manifest (clean/.../_runs/<run>.json), as jobs.clean_readings.run_id builds it
RUN = "{{ ('f3-' if params.source == 'f3' else 'f1-' ~ params.mode ~ '-') ~ (params.date or ds) }}"
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
    params={
        "date": Param(None, type=["null", "string"], format="date",
                      description="Ingest date of the raw partition to process (YYYY-MM-DD). Empty: the run's date."),
        "source": Param("f3", enum=["f3", "f1"], description="f3 = concentrators, f1 = Kaggle history"),
        "mode": Param("dev", enum=["dev", "full"], description="F1 only: dev = 4 dev blocks, full = 112 blocks"),
    },
    tags=["voltiagrid", "pipeline"],
) as dag:
    clean = job("clean_readings", f"--source {{{{ params.source }}}} --ingest-date {INGEST_DATE} --mode {{{{ params.mode }}}}")
    estimate = job("estimate_gaps", f"--run {RUN}")
    control = BashOperator(
        task_id="control_counts",
        bash_command=(
            "{% if params.source == 'f3' %}"
            f"cd $VOLTIA_REPO && python -m jobs.control_counts --ingest-date {INGEST_DATE} {LAKE}"
            "{% else %}echo 'control counts compare F3 raw with clean; nothing to check for F1'{% endif %}"
        ),
    )

    clean >> [estimate, control]
