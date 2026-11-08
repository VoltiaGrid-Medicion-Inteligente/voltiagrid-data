"""The on-demand quarterly report DAG (US-54, T-54.2): no schedule, a quarter parameter, one Spark job."""

import sys
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow.models")

DAGS = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS))


@pytest.fixture(scope="module")
def dag():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert bag.import_errors == {}
    return bag.dags["voltiagrid_quarterly_report"]


def test_runs_only_when_started_by_hand(dag):
    assert dag.schedule_interval is None


def test_takes_a_quarter_parameter(dag):
    assert "quarter" in dag.params


def test_one_task_runs_the_report_for_the_quarter(dag):
    assert list(dag.task_dict) == ["quarterly_report"]
    assert "--quarter {{ params.quarter }}" in dag.task_dict["quarterly_report"].bash_command
