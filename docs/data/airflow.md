# Airflow (US-32)

Airflow orchestrates the Spark jobs (ADR-004, ADR-014). Locally it runs with Docker Compose next to the voltiagrid-api
stack; in AWS it runs on the EC2 `t3.large` shared with RabbitMQ (ADR-014). DAGs live in `dags/` of this repo.

## 1. Airflow in Docker Compose (T-32.1)

| File | What |
|---|---|
| `airflow/Dockerfile` | `apache/airflow:2.10.5-python3.12` + Java 17 + PySpark 4.2.0 (same versions as `requirements.txt`), so a task can run the Spark jobs in local mode |
| `airflow/docker-compose.yml` | `airflow-db` (Postgres 16, metadata), `airflow-init` (migrations and admin user, runs once), `airflow-webserver` (http://localhost:8080), `airflow-scheduler`, and since T-32.2 two Celery workers and Flower |
| `airflow/.env.example` | Passwords and keys to copy to `airflow/.env` (ignored by git) |
| `dags/hello_voltiagrid.py` | Smoke test: Java, Spark and the lake from inside a task |

Inside every Airflow container:

| Path / variable | What |
|---|---|
| `/opt/airflow/dags` | `dags/` of this repo (edit a DAG and Airflow sees it in about 30 s) |
| `/opt/voltiagrid` (read-only), `PYTHONPATH` | this repo, so tasks run `python -m jobs.<job>` |
| `/lake`, `VOLTIA_LAKE` | the lake (`LAKE_DIR`, default the `lake/` of voltiagrid-api, where the F3 consumer writes) |
| `F5_URL`, `DATABASE_URL` | F5 API and F4 database of voltiagrid-api, through the `voltia` network |

### Start it

```powershell
# 1. voltiagrid-api stack up (creates the voltia network, RabbitMQ, database, F5)
cd ..\voltiagrid-api
make up
# 2. Airflow
cd ..\voltiagrid-data\airflow
copy .env.example .env      # change every change-me value
docker compose up -d --build
docker compose ps           # airflow-init: Exited (0); webserver and scheduler: (healthy)
```

Open http://localhost:8080 with `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`, turn on `hello_voltiagrid` and click
**Trigger DAG**: the three tasks (`java`, `spark`, `lake`) must end green.

The first build downloads about 1.5 GB (Airflow, Java, PySpark) and takes several minutes. `docker compose down` stops
it and keeps the metadata; `docker compose down -v` also deletes it (DAG runs and users).

### Checked in CI

The `dags` job of `.github/workflows/ci.yml` installs the same Airflow version (with its official constraints file),
runs `tests/test_dags.py` (every file in `dags/` imports without errors and each DAG has the expected tasks) and
validates `airflow/docker-compose.yml` with `docker compose config`. The `tests` job skips `test_dags.py` because it
does not install Airflow.

## 2. CeleryExecutor, RabbitMQ broker, Postgres metadata (T-32.2)

```
scheduler --(task message)--> RabbitMQ (voltiagrid-api, vhost /) --> airflow-worker-1 (worker-1) --+
                                                                  \--> airflow-worker-2 (worker-2) --+--> result + state
                                                                                                    in airflow-db (Postgres)
```

| Setting (`airflow/docker-compose.yml`) | Value | Why |
|---|---|---|
| `AIRFLOW__CORE__EXECUTOR` | `CeleryExecutor` | Tasks run in separate worker containers, several at a time (ADR-004) |
| `AIRFLOW__CELERY__BROKER_URL` | `amqp://<RABBITMQ_USER>:<RABBITMQ_PASSWORD>@rabbitmq:5672//` | The same RabbitMQ as the readings (ADR-001: one broker for both). Celery uses its own queue (`default`), separate from `readings.*` |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://airflow:...@airflow-db:5432/airflow` | Metadata (DAG runs, task states, users) in Postgres, never SQLite |
| `AIRFLOW__CELERY__RESULT_BACKEND` | `db+postgresql://airflow:...@airflow-db:5432/airflow` | Celery results in the same Postgres |
| `AIRFLOW__CELERY__WORKER_CONCURRENCY` | `2` (`AIRFLOW_WORKER_CONCURRENCY`) | Tasks per worker at the same time: 2 workers × 2 = 4 tasks in parallel |
| `SPARK_DRIVER_MEMORY` | `1g` | Memory of each Spark job (read by `jobs/spark.py`), so 4 parallel jobs fit in a 16 GB laptop |

The two workers have fixed host names (`worker-1`, `worker-2`): Airflow stores the host of each task instance and
Flower shows it, which is the evidence of T-32.3. Flower: http://localhost:5555.

In AWS (ADR-014) the same containers run on the EC2 `t3.large` with RabbitMQ; the metadata database moves to RDS.

### Check

```powershell
docker compose ps                      # airflow-worker-1, airflow-worker-2 and flower (healthy)
docker compose logs airflow-worker-1 | Select-String "ready"     # celery@worker-1 ready.
```

In the RabbitMQ console of voltiagrid-api (http://localhost:15672 → **Queues**) a `default` queue appears next to
`readings.live` and `readings.resend`, with 2 consumers (the workers).

## 3. Evidence: tasks spread over two workers in parallel (T-32.3)

`dags/celery_distribution_check.py`: 8 tasks of 20 seconds that can all start at once, then `report`, which reads from
the metadata database which worker ran each task and when, prints a table and **fails** if fewer than 2 workers took part
or no two tasks overlapped. So a green run is the evidence.

### How to get it

1. Stack up (sections 1 and 2) and http://localhost:8080 open.
2. Turn on `celery_distribution_check` and click **Trigger DAG**. It takes about 45 seconds.
3. **Grid** view → task `report` → **Logs**: copy the table into the PR.
4. Flower (http://localhost:5555) → **Tasks**: both `celery@worker-1` and `celery@worker-2` appear with tasks
   `succeeded`. Screenshot for the PR.
5. Same data straight from the metadata database:

```powershell
docker compose exec airflow-db psql -U airflow -d airflow -c "select task_id, hostname, start_date, end_date, state from task_instance where dag_id = 'celery_distribution_check' order by start_date desc limit 9"
```

### Reference run

Airflow 2.10.5, CeleryExecutor, RabbitMQ as broker and Postgres as metadata and result backend, two workers with
concurrency 2 (same settings as `airflow/docker-compose.yml`, run outside Docker on a Linux laptop):

| Task | Worker | Start (UTC) | End (UTC) | State |
|---|---|---|---|---|
| work_3 | worker-1 | 22:43:53 | 22:44:14 | success |
| work_1 | worker-1 | 22:43:53 | 22:44:14 | success |
| work_4 | worker-2 | 22:43:53 | 22:44:14 | success |
| work_2 | worker-2 | 22:43:53 | 22:44:14 | success |
| work_5 | worker-2 | 22:44:15 | 22:44:36 | success |
| work_6 | worker-2 | 22:44:16 | 22:44:36 | success |
| work_7 | worker-1 | 22:44:16 | 22:44:36 | success |
| work_8 | worker-1 | 22:44:16 | 22:44:36 | success |

`tasks per worker: {'worker-1': 4, 'worker-2': 4}; at most 4 tasks running at the same time`: 2 workers × concurrency 2
= 4 tasks at once, in two waves of 21 seconds.

With the Docker stack of the team laptop:

| Evidence | Value |
|---|---|
| `report` log: tasks per worker | `<worker-1: n, worker-2: n>` |
| `report` log: tasks at the same time | `<n>` |
| Flower screenshot | `<link or image in the PR>` |
| Run date | `<date>` |

Note: a DAG's `start_date` must be in the past. A run whose date is before `start_date` is created with no tasks and is
marked successful at once, which looks like it worked but ran nothing.

## 4. Daily readings pipeline (US-39)

### 4.1 DAG with dependencies and retries (T-39.1)

`dags/voltiagrid_readings.py`, every day at 01:00 UTC, for the F3 readings that reached raw the day before
(`ingest_date` = the run's logical date `ds`):

```
clean_readings --> estimate_gaps
              \--> control_counts      fails if a reading was lost or duplicated between raw and clean (CA-08)
```

Each task is a Spark job of this repo, run in the Celery worker as `cd $VOLTIA_REPO && python -m jobs.<job> ... --lake
$VOLTIA_LAKE`. The cleaning run writes its manifest `f3-<date>` and the next jobs read the dates from it (`--run`).

| Setting | Value | Why |
|---|---|---|
| `retries` | 2 | a failed task is tried twice more before the run fails |
| `retry_delay`, `retry_exponential_backoff`, `max_retry_delay` | 2 min, then 4, at most 10 | a broker or S3 hiccup has time to pass |
| `execution_timeout` | 40 min | a stuck job fails inside the 45 minutes of the daily close (US-55) |
| `max_active_runs` | 1 | two runs at once would overwrite the same partitions |
| `catchup` | False | turning the DAG on does not launch a run for every day since `start_date` |

A task runs only when everything upstream succeeded (default `all_success`): if `clean_readings` fails after its
retries, nothing downstream runs on half-written data.

### 4.2 Date and mode parameters (T-39.2)

| Parameter | Default | Effect |
|---|---|---|
| `date` | empty → the run's date (`ds`) | raw partition (`ingest_date`) to process. Only that period is read, and the next jobs process only the reading dates it touched (`--run`) |
| `source` | `f3` | `f3` = concentrator readings; `f1` = Kaggle history loaded to raw in US-23 |
| `mode` | `dev` | with `source = f1`: `dev` = the 4 blocks of the development subset, `full` = the 112 blocks (CA-09, in the cloud). Ignored for F3 |

The run manifest name follows: `f3-<date>`, or `f1-<mode>-<date>`, the same name `jobs.clean_readings` writes.
`control_counts` only applies to F3; with F1 it just logs that there is nothing to compare.

How to run a past day, or the F1 history:

- UI: **Trigger DAG** (play button) → **Trigger DAG w/ config** → change `date`, `source`, `mode` → **Trigger**.
- CLI (inside the stack):

```powershell
docker compose exec airflow-scheduler airflow dags trigger voltiagrid_readings -c '{\"date\": \"2026-11-02\"}'
docker compose exec airflow-scheduler airflow dags trigger voltiagrid_readings -c '{\"date\": \"2026-10-28\", \"source\": \"f1\", \"mode\": \"dev\"}'
```

Reference run (`airflow dags test voltiagrid_readings 2026-11-01 -c '{"date": "2026-10-31"}'` on the dev-seed lake): the
three tasks succeeded in 3 min 25 s; `clean_readings` processed only `ingest_date=2026-10-31` (322,308 rows in 36 reading
dates), `estimate_gaps` the same 36 dates, `control_counts` ended with `OK: nothing lost, nothing duplicated`. An earlier
attempt with a broken environment showed the retries: `clean_readings` failed, was retried after 2 and 4 minutes, and the
run failed without starting the next tasks.

### 4.3 Idempotency: rerunning causes no duplicates (T-39.3, CT-08)

A task retried by Airflow, the same date triggered twice, or a resend landing in a period already processed must not
duplicate anything. Each job overwrites only its own partitions (`partitionOverwriteMode=dynamic`) and recomputes them
from its inputs; the cleaning job merges with what clean already had (pipeline.md, section 4).

`tests/test_idempotency.py` builds a small lake (3 meters, 29 days of F3, one message stored twice, a null reading, a
90-minute gap and 4 missing days), runs the DAG's jobs in order (`clean_readings`, `estimate_gaps`, `circuit_load`,
`fact_consumption`, `billing`) **twice** for the same ingest date and checks:

| Check | Result |
|---|---|
| clean, filled, circuit_load, fact_consumption, billing_status, inspection_orders after the second run | identical row by row to the first (except `created_at` of the order, the time it was written) |
| one row per meter and interval in clean and filled | yes |
| the message stored twice | one set of 144 readings for that day |
| inspection orders | exactly one, `CU-MAC000003` for 2013-03 (13.8% estimated), not two |

The DAG reference run of 4.2 is the same check on the dev lake: processing `ingest_date=2026-10-31` again gave the same
322,308 clean rows and control counts with 0 duplicated.
