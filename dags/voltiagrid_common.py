"""How the VoltiaGrid DAGs start a job (US-40, T-40.3): in the Airflow worker, or as an ECS Fargate task.

VOLTIA_RUNTIME picks the runtime when the scheduler parses the DAG files:

    local (default)  every job runs in the worker with `python -m jobs.<job>` on the local lake (VOLTIA_LAKE)
    ecs              Spark jobs run as Fargate tasks of the Spark jobs image on the S3 lake (ADR-002, ADR-014);
                     the pull jobs (F4 export, F5 fetch) stay in the worker, as ADR-014 says, and write to raw in S3

ECS settings (outputs of voltiagrid-api/infra/envs/dev and the data stack):
    VOLTIA_ECS_CLUSTER       cluster, e.g. voltiagrid-dev
    VOLTIA_SPARK_TASK        task definition family, e.g. voltiagrid-dev-spark-jobs
    VOLTIA_SUBNETS           public subnets, comma separated
    VOLTIA_SECURITY_GROUP    the app Security Group
    VOLTIA_LAKE_RAW, VOLTIA_LAKE_CLEAN, VOLTIA_LAKE_CURATED   s3a:// URIs, for the jobs that run in the worker
The worker needs AWS credentials to start tasks (the instance role in AWS, an AWS profile locally).
"""

import os

from airflow.operators.bash import BashOperator

SPARK_CONTAINER = "spark-jobs"   # container name in the task definition (voltiagrid-api/infra/modules/spark_task)


def runtime() -> str:
    """Read on every call (not at import): the scheduler parses the DAG files again when they change."""
    return os.getenv("VOLTIA_RUNTIME", "local")


def lake_args() -> str:
    """Lake arguments of a job that runs in the worker."""
    if runtime() == "ecs":
        return "--raw $VOLTIA_LAKE_RAW --clean $VOLTIA_LAKE_CLEAN --curated $VOLTIA_LAKE_CURATED"
    return "--lake $VOLTIA_LAKE"


def worker_job(task_id: str, command: str) -> BashOperator:
    """A job that always runs in the worker (pull jobs): `command` is the python -m ... part, without lake args.
    The command runs from the repo folder, so relative paths like data/reference/tariffs.csv work everywhere."""
    return BashOperator(task_id=task_id, bash_command=f"cd $VOLTIA_REPO && {command} {lake_args()}")


def only_if(condition: str | None, command: str, otherwise: str) -> str:
    """Wrap a command in a Jinja condition: run it, or just echo why there is nothing to do."""
    if not condition:
        return command
    return f"{{% if {condition} %}}{command}{{% else %}}echo '{otherwise}'{{% endif %}}"


def spark_job(task_id: str, args: str, module: str | None = None, capacity: str = "FARGATE",
              cpu: int | None = None, memory: int | None = None, storage_gib: int | None = None,
              condition: str | None = None, otherwise: str = "nothing to do"):
    """A Spark job (`python -m jobs.<module or task_id> <args>`): in the worker, or a Fargate task on the S3 lake.

    On ECS the command goes through `sh -c` so the container expands its LAKE_* variables (set in the task
    definition); the operator waits for the task, streams its CloudWatch log into the Airflow log and fails the
    Airflow task if the container exits with an error, so retries work as in local mode. `capacity` is FARGATE
    (on-demand, the daily batches) or FARGATE_SPOT (only the history load, ADR-014); cpu and memory override the
    size of the task definition for one run (Spark gets 75% of the memory) and storage_gib its disk for Spark's
    scratch files (21 GiB by default). With `condition` (Jinja, e.g. "params.source == 'f3'") the job only
    runs when it is true; otherwise the task echoes `otherwise` and succeeds."""
    module = module or task_id
    if runtime() != "ecs":
        return BashOperator(task_id=task_id, bash_command=only_if(
            condition, f"cd $VOLTIA_REPO && python -m jobs.{module} {args} {lake_args()}", otherwise))

    from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

    cluster = os.environ["VOLTIA_ECS_CLUSTER"]
    command = only_if(condition, f"python -m jobs.{module} {args} --raw $LAKE_RAW --clean $LAKE_CLEAN "
                                 "--curated $LAKE_CURATED", otherwise)
    container = {"name": SPARK_CONTAINER, "command": ["sh", "-c", command]}
    overrides = {"containerOverrides": [container]}
    if cpu:
        overrides["cpu"] = str(cpu)
    if memory:
        overrides["memory"] = str(memory)
        container["environment"] = [{"name": "SPARK_DRIVER_MEMORY", "value": f"{memory * 3 // 4 // 1024}g"}]
    if storage_gib:
        overrides["ephemeralStorage"] = {"sizeInGiB": storage_gib}
    return EcsRunTaskOperator(
        task_id=task_id,
        cluster=cluster,
        task_definition=os.environ["VOLTIA_SPARK_TASK"],
        overrides=overrides,
        capacity_provider_strategy=[{"capacityProvider": capacity, "weight": 1}],
        network_configuration={"awsvpcConfiguration": {
            "subnets": os.environ["VOLTIA_SUBNETS"].split(","),
            "securityGroups": [os.environ["VOLTIA_SECURITY_GROUP"]],
            "assignPublicIp": "ENABLED",       # no NAT Gateway (ADR-011): ECR and CloudWatch over the internet
        }},
        awslogs_group=f"/ecs/{cluster}/spark-jobs",
        awslogs_stream_prefix=f"spark-jobs/{SPARK_CONTAINER}",
        awslogs_region=os.getenv("AWS_REGION", "us-east-1"),
        waiter_delay=15,
        waiter_max_attempts=4 * 60 * 6,   # 6 hours: the history load is long; execution_timeout ends it before
    )


def _task_rows(dag_run, dag, skip: str) -> list[dict]:
    """What each task of the run took and where it ran (Fargate size and capacity from the operator)."""
    rows = []
    for ti in dag_run.get_task_instances():
        if ti.task_id == skip or ti.duration is None:
            continue
        task = dag.get_task(ti.task_id)
        overrides = getattr(task, "overrides", None)
        if overrides is not None:     # EcsRunTaskOperator
            rows.append({"task_id": ti.task_id, "seconds": ti.duration, "where": "fargate",
                         "cpu": int(overrides.get("cpu", os.getenv("VOLTIA_SPARK_CPU", "2048"))),
                         "memory": int(overrides.get("memory", os.getenv("VOLTIA_SPARK_MEMORY", "8192"))),
                         "capacity": task.capacity_provider_strategy[0]["capacityProvider"]})
        else:
            rows.append({"task_id": ti.task_id, "seconds": ti.duration, "where": "worker", "cpu": 0, "memory": 0,
                         "capacity": ""})
    return sorted(rows, key=lambda r: r["task_id"])


def report_run_cost(dag_run=None, dag=None, task=None, **_):
    """Time, resources and cost of the run (T-41.2): logs the markdown of docs/data/cloud-run.md and returns the
    totals (XCom). Fargate time is the Airflow duration of each task, from its start to the end of the ECS task: a
    little more than what Fargate bills, so the cost is an upper bound."""
    from pipeline.costs import markdown, summarize

    summary = summarize(_task_rows(dag_run, dag, skip=task.task_id))
    started = min((ti.start_date for ti in dag_run.get_task_instances() if ti.start_date), default=None)
    ended = max((ti.end_date for ti in dag_run.get_task_instances() if ti.end_date), default=None)
    wall = (ended - started).total_seconds() if started and ended else summary["task_seconds"]
    print(markdown(summary, dag_run.run_id, wall))
    return {"wall_seconds": wall, "task_seconds": summary["task_seconds"], "cost_usd": round(summary["cost"], 4)}


def run_cost_task():
    """Last task of a DAG: runs even if a job failed (trigger_rule all_done), so a failed run also shows its cost."""
    from airflow.operators.python import PythonOperator

    return PythonOperator(task_id="run_cost", python_callable=report_run_cost, trigger_rule="all_done", retries=0)
