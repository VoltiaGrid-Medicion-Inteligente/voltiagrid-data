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
