"""Smoke test of the Airflow image (US-32, T-32.1): Java, PySpark and the VoltiaGrid repo are reachable from a task.

Trigger it by hand once after `docker compose up`; every task must end in success.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_voltiagrid",
    description="Checks Java, PySpark and the repo inside the Airflow image",
    start_date=datetime(2026, 10, 1),  # in the past: a run before start_date gets no tasks
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "voltiagrid"},   # US-49: every DAG has an owner (test_dag_integrity.py)
    tags=["voltiagrid", "smoke"],
) as dag:
    java = BashOperator(task_id="java", bash_command="java -version")
    spark = BashOperator(
        task_id="spark",
        bash_command=(
            "cd $VOLTIA_REPO && python -c \"from jobs.spark import get_spark; "
            "s = get_spark('hello', ui=False); print('spark', s.version, s.range(10).count()); s.stop()\""
        ),
    )
    lake = BashOperator(task_id="lake", bash_command="ls -la $VOLTIA_LAKE && test -w $VOLTIA_LAKE")
    java >> spark >> lake
