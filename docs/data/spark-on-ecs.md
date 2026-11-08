# Spark on ECS Fargate (sprint 5)

ADR-002 and ADR-014: the Spark jobs run in **local mode inside one container** (one process with every core of the
task), on Fargate on-demand for the daily batches and on Fargate Spot for the one-off F1 history (US-41). Airflow only
starts the tasks. The lake is the three S3 buckets of `voltiagrid-api/infra/envs/data` (raw, clean, curated).

## 1. The image (US-40, T-40.1)

`docker/spark/Dockerfile` builds `voltiagrid/spark-jobs`: Python 3.12, Java 17, the versions of `requirements.txt` and
the code of `jobs/`, `pipeline/`, `schemas/`, `data/reference/` and `dev_subset.py` (the F1 dev blocks).

The **S3A connector** (`s3a://` paths) is not part of PySpark. `docker/spark/fetch_s3a_jars.py` asks Spark at build
time for `org.apache.hadoop:hadoop-aws` of the same Hadoop version that PySpark ships (3.5.0 with PySpark 4.2.0) and
copies it, with the AWS SDK version it was built with, into PySpark's `jars/` folder. No version is written by hand,
so upgrading PySpark upgrades the connector. The AWS SDK bundle is large: the image is about 1.5 GB (ECR storage
about 0.15 USD a month; Fargate pulls it in about a minute).

```powershell
docker build -f docker/spark/Dockerfile -t voltiagrid/spark-jobs:local .
docker run --rm voltiagrid/spark-jobs:local                      # java, spark, hadoop versions and the S3A class
docker run --rm voltiagrid/spark-jobs:local python -m jobs.clean_readings --help
```

**Credentials.** None in the image or in the Spark configuration. S3A's default chain finds them: on Fargate the ECS
task role (T-40.2); on a laptop `AWS_PROFILE` or the `AWS_*` variables. `jobs/spark.py` only adds the region of the
buckets (`AWS_REGION`) and, for an S3-compatible server such as MinIO, `S3_ENDPOINT` with path-style URLs.

**CI.** `.github/workflows/spark-image.yml` builds the image when `docker/spark/`, the jobs or `requirements.txt`
change, starts a local S3 server (moto, installed from PyPI), and runs `docker/spark/check_image.py`: Spark writes 5 rows as Parquet to
`s3a://voltiagrid-ci/_check` and reads them back. That is the same code path as the lake buckets, without AWS
credentials in the CI. (MinIO no longer publishes its images on Docker Hub, so the check uses moto.)

The container runs as user `spark` (uid 10001), not root. The jobs only need `/tmp` (Spark's scratch space).

## 2. Airflow starts the jobs on Fargate (US-40, T-40.3)

`dags/voltiagrid_common.py` decides how each task of the DAGs runs, from `VOLTIA_RUNTIME`:

| Task | `local` (default) | `ecs` |
|---|---|---|
| Spark jobs (`clean_readings`, `estimate_gaps`, `control_counts`, `circuit_load`, `fact_consumption`, `billing`, `transformer_losses`, `suspicious_meters`) | `python -m jobs.<job> … --lake $VOLTIA_LAKE` in the worker | `EcsRunTaskOperator`: a Fargate task of `voltiagrid-dev-spark-jobs` (on-demand) with `sh -c "python -m jobs.<job> … --raw $LAKE_RAW --clean $LAKE_CLEAN --curated $LAKE_CURATED"` |
| Pull jobs (`export_inventory`, `fetch_f5`) | in the worker, local lake | in the worker (ADR-014), writing raw in S3 (`--raw $VOLTIA_LAKE_RAW …`) |

On ECS the operator waits for the task, copies its CloudWatch log into the Airflow log of the task, and fails the
Airflow task if the container exits with an error, so the retries, backoff and 40-minute timeout of the DAG work the
same as in local mode. The grid, the dependencies and the parameters do not change.

**Try it from the local Airflow** (the dev stack of voltiagrid-api applied, the Spark image published, T-40.2):

1. In `airflow/.env` set `VOLTIA_RUNTIME=ecs` and the values from `terraform -chdir=infra/envs/dev output` in
   voltiagrid-api (`ecs_cluster`, `spark_task_family`, `public_subnet_ids` joined by commas, `app_security_group_id`)
   and the three bucket names of the data stack.
2. `aws sso login --profile voltia-admin` on the laptop: the workers use that session through the mounted `~/.aws`.
3. `docker compose up -d --build` (the image now has the Amazon provider).
4. Trigger `voltiagrid_readings` with a date whose raw partition is in S3. In the Grid, the log of a Spark task shows
   `Starting ECS Task`, the task ARN, the job's own log lines and `ECS Task has been successfully executed`.

`tests/test_dags.py` checks both runtimes: on ECS every Spark task is a Fargate task of the Spark image with the
cluster, subnets, Security Group and log group given, and its command reads and writes the S3 lake.

## 3. The F1 history in the cloud (US-41)

### 3.1 DAG `voltiagrid_history` (T-41.1)

Started by hand once F1 is in raw (US-23). Same jobs as the daily DAG, in the order that takes the history to curated:

```
clean_readings (F1, --mode full) --> estimate_gaps --> fact_consumption (curated, monthly)
export_inventory ----------------------------------/
```

| | Daily DAG | History DAG |
|---|---|---|
| Capacity | Fargate on-demand (the close must finish in 45 minutes, US-55) | **Fargate Spot** (ADR-014): about 70% cheaper; an interruption is retried from scratch |
| Task size | 2 vCPU, 8 GB, 21 GiB disk (task definition) | 8 vCPU, 32 GB (Spark gets 24 GB), 100 GiB disk, as overrides of the same task definition |
| Timeout per task | 40 minutes | 4 hours |

Trigger it with the date of the F1 upload (Airflow UI → **Trigger DAG w/ config**, or):

```powershell
docker compose exec airflow-scheduler airflow dags trigger voltiagrid_history -c '{\"ingest_date\": \"2026-10-28\", \"mode\": \"full\"}'
```

Locally (`VOLTIA_RUNTIME=local`) use `mode=dev`: the 4 dev blocks fit a laptop, the full history does not.

### 3.2 Time and cost of every run (T-41.2)

Both DAGs end with `run_cost` (`trigger_rule = all_done`: it runs even if a job failed). It reads the task instances
of the run and prints, for `docs/data/cloud-run.md`:

- **Duration**: wall clock of the run and minutes of each task;
- **Resources**: where each task ran (Fargate on-demand or Spot, vCPU and GB; or the Airflow worker);
- **Cost**: each Fargate task's minutes × (vCPU × price + GB × price) with the prices in `pipeline/costs.py`
  (us-east-1, Linux/x86; one minute at least). Worker tasks add nothing: the instance is paid by the hour anyway.

The totals also go to XCom (`wall_seconds`, `task_seconds`, `cost_usd`). Airflow's duration includes starting the
Fargate task, a little more than Fargate bills, so the cost is an upper bound. CA-09's test checks that
`docs/data/cloud-run.md` has the Duration, Resources and Cost sections; the numbers are the ones of the real run.

### 3.3 Control counts between layers (T-41.3)

`jobs/layer_counts.py` checks one cleaning run from raw to curated:

| Step | Must hold |
|---|---|
| raw → clean | every meter-interval of raw (valid clock) is in clean exactly once (`lost = 0`, `duplicated = 0`, the CA-08 check) |
| clean → filled | `filled_real` = `clean_valid`: every valid reading kept as a real reading; the rest of filled is estimated |
| filled → curated | in the run's months, the intervals and kWh of `fact_consumption` equal the filled ones of the meters F4 has (kWh within 0.01%, the monthly fact rounds to 4 decimals) |

`meters_without_f4` counts the meters with readings that F4 does not have: they cannot be billed, so they are
reported and left out of the comparison. It prints one line per count and `OK: raw, clean, filled and curated
agree`, or `CHECK FAILED` with the reason and exit code 1.

```powershell
python -m jobs.layer_counts --run f1-full-2026-10-28 --raw s3a://<raw bucket> --clean s3a://<clean bucket> --curated s3a://<curated bucket>
.\infra\scripts\run-spark.ps1 -Job layer_counts -JobArgs "--run f1-full-2026-10-28"      # from voltiagrid-api, on Fargate
```

The output goes into *Control counts between layers* of `docs/data/cloud-run.md`.
