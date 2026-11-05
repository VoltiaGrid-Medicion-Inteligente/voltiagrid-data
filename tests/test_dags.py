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


# ---------- voltiagrid_readings (US-39) ----------

def test_readings_dag_cleans_first_then_estimates_and_checks_control_counts(dagbag):
    dag = dagbag.dags["voltiagrid_readings"]
    assert dag.task_dict["clean_readings"].downstream_task_ids >= {"estimate_gaps", "control_counts"}
    assert dag.task_dict["estimate_gaps"].upstream_task_ids == {"clean_readings"}
    assert dag.max_active_runs == 1


def test_every_pipeline_task_retries_with_backoff_and_has_a_timeout(dagbag):
    dag = dagbag.dags["voltiagrid_readings"]
    for task in dag.tasks:
        assert task.retries >= 2, task.task_id
        assert task.retry_exponential_backoff, task.task_id
        assert task.execution_timeout is not None, task.task_id


def test_tasks_run_the_jobs_of_this_repo_on_the_lake(dagbag):
    dag = dagbag.dags["voltiagrid_readings"]
    command = dag.task_dict["clean_readings"].bash_command
    assert command.startswith("cd $VOLTIA_REPO && python -m jobs.clean_readings")
    assert "--lake $VOLTIA_LAKE" in command


def render(dag, task_id, ds="2026-11-02", **params):
    values = {"date": None, "source": "f3", "mode": "dev"} | params
    return dag.get_template_env().from_string(dag.task_dict[task_id].bash_command).render(params=values, ds=ds)


def test_without_a_date_the_run_processes_its_own_day(dagbag):
    dag = dagbag.dags["voltiagrid_readings"]
    assert "--source f3 --ingest-date 2026-11-02 --mode dev" in render(dag, "clean_readings")
    assert "--run f3-2026-11-02" in render(dag, "estimate_gaps")
    assert "--ingest-date 2026-11-02" in render(dag, "control_counts")


def test_the_date_parameter_processes_only_that_partition(dagbag):
    dag = dagbag.dags["voltiagrid_readings"]
    assert "--ingest-date 2026-10-15 " in render(dag, "clean_readings", date="2026-10-15")
    assert "--run f3-2026-10-15 " in render(dag, "estimate_gaps", date="2026-10-15")


def test_f1_history_uses_the_mode_and_the_f1_run_name(dagbag):
    dag = dagbag.dags["voltiagrid_readings"]
    assert "--source f1 --ingest-date 2026-10-28 --mode full" in render(dag, "clean_readings", source="f1",
                                                                      mode="full", date="2026-10-28")
    # same name as jobs.clean_readings.run_id (this CI job has no PySpark to import it)
    assert "--run f1-full-2026-10-28 " in render(dag, "estimate_gaps", source="f1", mode="full", date="2026-10-28")
    assert "nothing to check for F1" in render(dag, "control_counts", source="f1")


def test_parameters_have_safe_defaults(dagbag):
    params = dagbag.dags["voltiagrid_readings"].params
    assert (params["date"], params["source"], params["mode"]) == (None, "f3", "dev")
