# The F1 history in the cloud (US-41, CA-09)

CA-09: the full F1 history is processed in the cloud, reporting time, resources and cost. This page is that report:
the run of the DAG `voltiagrid_history` (`mode = full`) with `VOLTIA_RUNTIME=ecs`, and the control counts between
layers (T-41.3).

How to fill it: the last task of the run, `run_cost`, prints the three sections below ready to paste (Airflow → the
run → `run_cost` → **Logs**). Replace the tables of this page with that output; do not edit the numbers by hand.

- **Date of the run:** <fill: YYYY-MM-DD>
- **Run:** <fill: run id, e.g. manual__2026-11-10T02:00:00+00:00>
- **Input:** `raw/source=f1/dataset=halfhourly/ingest_date=<fill>/`, 112 blocks (about 167 million readings)
- **Image:** `voltiagrid/spark-jobs:<fill: tag>`

## Duration

| Task | Minutes |
|---|---|
| <paste the run_cost output> | |

## Resources

| Task | Runs on | vCPU | Memory (GB) |
|---|---|---|---|
| `clean_readings`, `estimate_gaps`, `fact_consumption` | FARGATE_SPOT | 8 | 32 |
| `export_inventory`, `run_cost` | Airflow worker | — | — |

Each Spark task: 100 GiB of scratch disk; Spark in local mode with 24 GB of driver memory (ADR-014).

## Cost

| Task | USD |
|---|---|
| <paste the run_cost output> | |

Fargate time is the Airflow duration of each task, so the cost is an upper bound. The monthly estimate
(voltiagrid-docs, *Costs → Monthly estimate*) assumed this load once; compare it with the total above.

## Control counts between layers

<paste the output of `python -m jobs.layer_counts --run f1-full-<date>` (T-41.3)>
