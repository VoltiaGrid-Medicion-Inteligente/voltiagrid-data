# Airflow (US-32)

Airflow orchestrates the Spark jobs (ADR-004, ADR-014). Locally it runs with Docker Compose next to the voltiagrid-api
stack; in AWS it runs on the EC2 `t3.large` shared with RabbitMQ (ADR-014). DAGs live in `dags/` of this repo.

## 1. Airflow in Docker Compose (T-32.1)

| File | What |
|---|---|
| `airflow/Dockerfile` | `apache/airflow:2.10.5-python3.12` + Java 17 + PySpark 4.2.0 (same versions as `requirements.txt`), so a task can run the Spark jobs in local mode |
| `airflow/docker-compose.yml` | `airflow-db` (Postgres 16, metadata), `airflow-init` (migrations and admin user, runs once), `airflow-webserver` (http://localhost:8080), `airflow-scheduler` |
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
