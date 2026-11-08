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
        if task.task_id == "run_cost":   # only reports; nothing to retry
            continue
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


def test_each_job_waits_for_the_data_it_reads(dagbag):
    """T-39.4: every job of the sprint is in the DAG, after the jobs that write its inputs."""
    tasks = dagbag.dags["voltiagrid_readings"].task_dict
    upstream = {name: task.upstream_task_ids for name, task in tasks.items()}
    assert upstream["estimate_gaps"] == {"clean_readings"}
    assert upstream["control_counts"] == {"clean_readings"}
    assert upstream["fetch_f5"] == {"clean_readings"}
    assert upstream["circuit_load"] == {"export_inventory", "estimate_gaps"}
    assert upstream["fact_consumption"] == {"export_inventory", "estimate_gaps"}
    assert upstream["billing"] == {"fact_consumption"}
    assert upstream["transformer_losses"] == {"export_inventory", "estimate_gaps", "fetch_f5"}
    assert upstream["suspicious_meters"] == {"transformer_losses"}
    assert upstream["export_inventory"] == set() and upstream["clean_readings"] == set()


def test_jobs_after_cleaning_read_the_dates_of_the_cleaning_run(dagbag):
    dag = dagbag.dags["voltiagrid_readings"]
    for task_id in ("circuit_load", "fact_consumption", "billing", "fetch_f5", "transformer_losses",
                    "suspicious_meters"):
        assert "--run f3-2026-11-02 " in render(dag, task_id), task_id


# ---------- runtime on ECS Fargate (US-40, T-40.3) ----------

ECS_ENV = {"VOLTIA_RUNTIME": "ecs", "VOLTIA_ECS_CLUSTER": "voltiagrid-dev", "VOLTIA_SPARK_TASK": "voltiagrid-dev-spark-jobs",
           "VOLTIA_SUBNETS": "subnet-a,subnet-b", "VOLTIA_SECURITY_GROUP": "sg-app", "AWS_REGION": "us-east-1"}


@pytest.fixture(scope="module")
def ecs_dagbag():
    import os

    from airflow.models import DagBag

    saved = {k: os.environ.get(k) for k in ECS_ENV}
    os.environ.update(ECS_ENV)
    try:
        yield DagBag(dag_folder=str(DAGS), include_examples=False)
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def ecs_command(dag, task_id, ds="2026-11-02", **params):
    values = {"date": None, "source": "f3", "mode": "dev"} | params
    command = dag.task_dict[task_id].overrides["containerOverrides"][0]["command"][2]
    return dag.get_template_env().from_string(command).render(params=values, ds=ds)


def test_ecs_runtime_loads_without_errors(ecs_dagbag):
    assert ecs_dagbag.import_errors == {}


def test_on_ecs_spark_jobs_are_fargate_tasks_of_the_spark_image(ecs_dagbag):
    from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

    dag = ecs_dagbag.dags["voltiagrid_readings"]
    spark = ["clean_readings", "estimate_gaps", "control_counts", "circuit_load", "fact_consumption", "billing",
             "transformer_losses", "suspicious_meters"]
    for task_id in spark:
        task = dag.task_dict[task_id]
        assert isinstance(task, EcsRunTaskOperator), task_id
        assert (task.cluster, task.task_definition) == ("voltiagrid-dev", "voltiagrid-dev-spark-jobs")
        assert task.capacity_provider_strategy == [{"capacityProvider": "FARGATE", "weight": 1}]   # on-demand
        net = task.network_configuration["awsvpcConfiguration"]
        assert (net["subnets"], net["securityGroups"], net["assignPublicIp"]) == (["subnet-a", "subnet-b"], ["sg-app"],
                                                                                    "ENABLED")
        assert task.awslogs_group == "/ecs/voltiagrid-dev/spark-jobs"
        assert task.retries >= 2 and task.execution_timeout is not None   # same retries as local mode


def test_on_ecs_the_job_reads_and_writes_the_s3_lake_of_the_task(ecs_dagbag):
    dag = ecs_dagbag.dags["voltiagrid_readings"]
    command = ecs_command(dag, "clean_readings", date="2026-11-04")
    assert command.startswith("python -m jobs.clean_readings --source f3 --ingest-date 2026-11-04 --mode dev")
    assert command.endswith("--raw $LAKE_RAW --clean $LAKE_CLEAN --curated $LAKE_CURATED")
    assert "--run f3-2026-11-04 " in ecs_command(dag, "estimate_gaps", date="2026-11-04")
    assert "nothing to check for F1" in ecs_command(dag, "control_counts", source="f1")


def test_on_ecs_pull_jobs_stay_in_the_worker_and_write_raw_in_s3(ecs_dagbag):
    dag = ecs_dagbag.dags["voltiagrid_readings"]
    for task_id in ("export_inventory", "fetch_f5"):
        command = dag.task_dict[task_id].bash_command
        assert command.startswith("cd $VOLTIA_REPO && python -m jobs."), task_id
        assert "--raw $VOLTIA_LAKE_RAW --clean $VOLTIA_LAKE_CLEAN --curated $VOLTIA_LAKE_CURATED" in command


def test_local_runtime_is_still_the_default(dagbag):
    from airflow.operators.bash import BashOperator

    assert all(isinstance(t, BashOperator) for t in dagbag.dags["voltiagrid_readings"].tasks if t.task_id != "run_cost")


# ---------- voltiagrid_history (US-41, T-41.1) ----------

def test_history_goes_from_raw_to_curated_in_order(dagbag):
    dag = dagbag.dags["voltiagrid_history"]
    upstream = {name: task.upstream_task_ids for name, task in dag.task_dict.items()}
    assert upstream["estimate_gaps"] == {"clean_readings"}
    assert upstream["fact_consumption"] == {"estimate_gaps", "export_inventory"}
    assert dag.schedule_interval is None and dag.max_active_runs == 1     # only by hand, one at a time


def test_history_reads_f1_of_the_upload_date_and_names_the_run_like_the_clean_job(dagbag):
    dag = dagbag.dags["voltiagrid_history"]
    values = {"ingest_date": "2026-10-28", "mode": "full"}
    env = dag.get_template_env()
    clean = env.from_string(dag.task_dict["clean_readings"].bash_command).render(params=values)
    assert "python -m jobs.clean_readings --source f1 --ingest-date 2026-10-28 --mode full" in clean
    estimate = env.from_string(dag.task_dict["estimate_gaps"].bash_command).render(params=values)
    assert "--run f1-full-2026-10-28 " in estimate


def test_on_ecs_the_history_runs_on_spot_with_a_bigger_task(ecs_dagbag):
    dag = ecs_dagbag.dags["voltiagrid_history"]
    for task_id in ("clean_readings", "estimate_gaps", "fact_consumption"):
        task = dag.task_dict[task_id]
        assert task.capacity_provider_strategy == [{"capacityProvider": "FARGATE_SPOT", "weight": 1}], task_id
        assert (task.overrides["cpu"], task.overrides["memory"]) == ("8192", "32768")
        assert task.overrides["ephemeralStorage"] == {"sizeInGiB": 100}
        assert task.overrides["containerOverrides"][0]["environment"] == [{"name": "SPARK_DRIVER_MEMORY",
                                                                           "value": "24g"}]
        assert task.retries >= 2                       # a Spot interruption is retried


# ---------- time and cost of every run (T-41.2) ----------

def test_both_dags_end_with_the_cost_report_even_if_a_job_fails(dagbag):
    for dag_id in ("voltiagrid_readings", "voltiagrid_history"):
        dag = dagbag.dags[dag_id]
        cost = dag.task_dict["run_cost"]
        assert cost.trigger_rule == "all_done", dag_id
        assert cost.downstream_task_ids == set(), dag_id
        assert {t.task_id for t in dag.tasks if not t.downstream_task_ids} == {"run_cost"}, dag_id


def test_cost_report_prices_the_fargate_tasks_of_a_run(ecs_dagbag, capsys):
    from datetime import datetime, timedelta, timezone

    from voltiagrid_common import report_run_cost

    dag = ecs_dagbag.dags["voltiagrid_history"]
    t0 = datetime(2026, 11, 10, 2, 0, tzinfo=timezone.utc)

    class TI:
        def __init__(self, task_id, minutes, offset):
            self.task_id, self.duration = task_id, minutes * 60
            self.start_date = t0 + timedelta(minutes=offset)
            self.end_date = self.start_date + timedelta(minutes=minutes)

    class Run:
        run_id = "manual__2026-11-10"

        def get_task_instances(self):
            return [TI("clean_readings", 60, 0), TI("export_inventory", 1, 0), TI("estimate_gaps", 30, 60),
                    TI("fact_consumption", 10, 90), TI("run_cost", 0, 100)]

    totals = report_run_cost(dag_run=Run(), dag=dag, task=dag.task_dict["run_cost"])
    assert totals["wall_seconds"] == 100 * 60
    assert totals["task_seconds"] == 101 * 60
    assert totals["cost_usd"] == round(100 / 60 * (8 * 0.0127 + 32 * 0.0014), 4)    # 100 min of Spot 8 vCPU / 32 GB
    out = capsys.readouterr().out
    assert "## Duration" in out and "| `export_inventory` | Airflow worker | — | — |" in out
