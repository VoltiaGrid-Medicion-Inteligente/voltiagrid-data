"""Every DAG is valid before it reaches Airflow (US-49, T-49.2). Runs in the CI job `dags`, with Airflow installed.

test_dags.py checks what each DAG does; these checks apply to all of them, including the ones still to come:
a DAG that does not import, has a cycle, starts a job that does not exist or is slow to parse fails the CI.
"""

import re
import sys
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow.models")

REPO = Path(__file__).resolve().parents[1]
DAGS = REPO / "dags"
sys.path.insert(0, str(DAGS))
JOB = re.compile(r"python -m (jobs\.[a-z_0-9]+)")
MAX_PARSE_SECONDS = 2.0     # the scheduler parses every file again and again: no Spark or data at import time


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_a_dag_that_does_not_import_is_reported(tmp_path):
    """The check really fails: a DAG file with an error ends up in import_errors, not silently ignored."""
    from airflow.models import DagBag

    (tmp_path / "broken_dag.py").write_text("from airflow import DAG\nimport module_that_does_not_exist\n")
    bag = DagBag(dag_folder=str(tmp_path), include_examples=False)
    assert list(bag.import_errors) == [str(tmp_path / "broken_dag.py")]


def test_no_import_errors_and_every_file_defines_a_dag(dagbag):
    assert dagbag.import_errors == {}
    files = {Path(d.fileloc).name for d in dagbag.dags.values()}
    expected = {p.name for p in DAGS.glob("*.py") if "DAG(" in p.read_text(encoding="utf-8")}
    assert files == expected


def test_no_dag_has_a_cycle(dagbag):
    from airflow.utils.dag_cycle_tester import check_cycle

    for dag in dagbag.dags.values():
        check_cycle(dag)          # raises AirflowDagCycleException


def test_every_dag_has_owner_tags_and_one_run_at_a_time(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.owner == "voltiagrid", dag_id
        assert "voltiagrid" in dag.tags, dag_id
        assert dag.max_active_runs == 1, dag_id      # two runs would overwrite the same partitions
        assert dag.catchup is False, dag_id          # never a backlog of runs after a pause


def task_commands(task) -> list[str]:
    commands = []
    if hasattr(task, "bash_command"):
        commands.append(task.bash_command)
    overrides = getattr(task, "overrides", None)
    if overrides:
        commands += [" ".join(c.get("command", [])) for c in overrides.get("containerOverrides", [])]
    return commands


def test_every_job_a_task_starts_exists_in_this_repo(dagbag):
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            for command in task_commands(task):
                for module in JOB.findall(command):
                    path = REPO / (module.replace(".", "/") + ".py")
                    assert path.exists(), f"{dag.dag_id}.{task.task_id} starts {module}, which does not exist"


def test_dag_files_parse_fast(dagbag):
    slow = {s.file: s.duration.total_seconds() for s in dagbag.dagbag_stats
            if s.duration.total_seconds() > MAX_PARSE_SECONDS}
    assert slow == {}
