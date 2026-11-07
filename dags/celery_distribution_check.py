"""Evidence that the CeleryExecutor spreads tasks over at least 2 workers in parallel (US-32, T-32.3).

8 tasks that each take 20 seconds start at the same time; `report` then reads from the metadata database which worker
ran each one and when, prints the table for the PR and fails if fewer than 2 workers took part or nothing overlapped.
Trigger it by hand (it has no schedule).
"""

from collections import Counter
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PARALLEL_TASKS = 8
SECONDS_PER_TASK = 20


def max_overlap(intervals: list[tuple[datetime, datetime]]) -> int:
    """Largest number of intervals running at the same moment."""
    events = sorted([(s, 1) for s, _ in intervals] + [(e, -1) for _, e in intervals], key=lambda x: (x[0], x[1]))
    running = peak = 0
    for _, step in events:
        running += step
        peak = max(peak, running)
    return peak


def report(dag_id: str, run_id: str) -> dict:
    from airflow.models import TaskInstance
    from airflow.utils.session import create_session

    with create_session() as session:
        tis = (session.query(TaskInstance)
               .filter(TaskInstance.dag_id == dag_id, TaskInstance.run_id == run_id,
                       TaskInstance.task_id.like("work_%"))
               .order_by(TaskInstance.start_date).all())
        rows = [(ti.task_id, ti.hostname, ti.start_date, ti.end_date, ti.state) for ti in tis]
    print("| Task | Worker | Start (UTC) | End (UTC) | State |")
    print("|---|---|---|---|---|")
    for task, host, start, end, state in rows:
        print(f"| {task} | {host} | {start:%H:%M:%S} | {end:%H:%M:%S} | {state} |")
    hosts = Counter(host for _, host, _, _, _ in rows)
    overlap = max_overlap([(s, e) for _, _, s, e, _ in rows])
    print(f"tasks per worker: {dict(hosts)}; at most {overlap} tasks running at the same time")
    if len(hosts) < 2:
        raise AirflowFailException(f"only {len(hosts)} worker ran the tasks: {dict(hosts)}")
    if overlap < 2:
        raise AirflowFailException("no two tasks ran at the same time")
    return {"workers": dict(hosts), "max_parallel": overlap}


with DAG(
    dag_id="celery_distribution_check",
    description="Shows that tasks run on several Celery workers at the same time",
    start_date=datetime(2026, 10, 1),  # in the past: a run before start_date gets no tasks
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "voltiagrid"},   # US-49: every DAG has an owner (test_dag_integrity.py)
    max_active_tasks=PARALLEL_TASKS,
    tags=["voltiagrid", "evidence"],
) as dag:
    work = [
        BashOperator(task_id=f"work_{i}", bash_command=f"echo \"running on $(hostname)\"; sleep {SECONDS_PER_TASK}")
        for i in range(1, PARALLEL_TASKS + 1)
    ]
    summary = PythonOperator(
        task_id="report",
        python_callable=report,
        op_kwargs={"dag_id": "{{ dag.dag_id }}", "run_id": "{{ run_id }}"},
    )
    work >> summary
