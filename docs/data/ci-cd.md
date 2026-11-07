# CI/CD of the data pipeline (US-49)

What the CI of voltiagrid-data checks on every pull request, so a broken job, DAG or dataset never reaches `develop`,
and how the artifacts of each merge to `main` are versioned.

| Check (CI job) | What it proves | Card |
|---|---|---|
| `lint` | ruff: no unused imports, no undefined names | — |
| `tests` | Unit tests of the rules and the jobs on small DataFrames (`pytest -m "not smoke"`) | sprints 2–5 |
| `pipeline-smoke` | Every job of the daily DAG **from its command line**, in order, on a small lake | T-49.1 |
| `dags` | The DAGs import, and their structure is right | T-49.2 |
| `secrets` | gitleaks: no credentials in the history | — |

## 1. Jobs tested with small data (T-49.1)

`tests/test_pipeline_smoke.py` builds a lake of 3 meters and 29 days (F3 with a null reading and a 90-minute gap, F4
meters and contracts, F5 of the same days) and runs, each in its own process as Airflow does:

```
clean_readings -> estimate_gaps -> control_counts -> circuit_load -> fact_consumption -> billing
              -> transformer_losses -> suspicious_meters
```

A job that exits with an error fails the check and its output appears in the log. It catches what the unit tests do
not reach: arguments, the run manifest, a job reading what the previous one wrote. A second test checks that a wrong
argument (a run that does not exist) stops the job with an error instead of writing nothing silently.

It takes about 2–3 minutes (Spark starts once per job), so it is marked `smoke` and has its own CI job; the `tests`
job skips it. Locally:

```powershell
python -m pytest -m smoke -v
```
