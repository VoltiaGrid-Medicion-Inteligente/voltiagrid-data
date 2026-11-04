"""DAGs load without errors (ADR-004: DAGs are validated in CI). Skipped where Airflow is not installed; the CI job
`dags` installs it."""

from pathlib import Path

import pytest

# "airflow.models", not "airflow": the airflow/ folder of this repo (Dockerfile, compose) would import as an empty
# namespace package where Airflow is not installed.
airflow = pytest.importorskip("airflow.models")

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_every_dag_file_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_hello_dag_checks_java_spark_and_the_lake_in_order(dagbag):
    dag = dagbag.dags["hello_voltiagrid"]  # .dags, not get_dag(): no metadata database needed
    assert [t.task_id for t in dag.topological_sort()] == ["java", "spark", "lake"]
