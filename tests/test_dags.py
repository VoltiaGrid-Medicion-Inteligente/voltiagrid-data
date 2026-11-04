"""DAGs load without errors (ADR-004: DAGs are validated in CI). Skipped where Airflow is not installed; the CI job
`dags` installs it."""

import sys
from pathlib import Path

import pytest

# "airflow.models", not "airflow": the airflow/ folder of this repo (Dockerfile, compose) would import as an empty
# namespace package where Airflow is not installed.
airflow = pytest.importorskip("airflow.models")

DAGS = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS))


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_every_dag_file_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_hello_dag_checks_java_spark_and_the_lake_in_order(dagbag):
    dag = dagbag.dags["hello_voltiagrid"]  # .dags, not get_dag(): no metadata database needed
    assert [t.task_id for t in dag.topological_sort()] == ["java", "spark", "lake"]


def test_distribution_check_runs_8_parallel_tasks_then_the_report(dagbag):
    dag = dagbag.dags["celery_distribution_check"]
    work = [t for t in dag.tasks if t.task_id.startswith("work_")]
    assert len(work) == 8
    assert all(t.downstream_task_ids == {"report"} for t in work)
    assert dag.max_active_tasks >= 8


def test_max_overlap_counts_tasks_running_at_the_same_moment():
    from datetime import datetime

    import celery_distribution_check as check

    t = datetime(2026, 11, 1, 10, 0, 0)
    s = lambda sec: t.replace(second=sec)  # noqa: E731
    assert check.max_overlap([(s(0), s(20)), (s(5), s(25)), (s(30), s(40))]) == 2
    assert check.max_overlap([(s(0), s(10)), (s(10), s(20))]) == 1   # one ends as the other starts
