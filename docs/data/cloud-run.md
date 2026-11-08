# The F1 history in the cloud (US-41, CA-09)

CA-09: the full F1 history is processed in the cloud, reporting time, resources and cost. This page is that report:
the run of the DAG `voltiagrid_history` (`mode = full`) with `VOLTIA_RUNTIME=ecs`, and the control counts between
layers (T-41.3).

How to fill it: the last task of the run, `run_cost`, prints the three sections below ready to paste (Airflow → the
run → `run_cost` → **Logs**). Replace the tables of this page with that output; do not edit the numbers by hand.

- **Date of the run:** 2026-11-08
- **Run:** manual__2026-11-08T02:46:13+00:00
- **Input:** `raw/source=f1/dataset=halfhourly/ingest_date=2026-10-28/`, 112 blocks (about 167 million readings)
- **Image:** `voltiagrid/spark-jobs:69c7d44`

**Status: not complete.** `clean_readings` processed the whole history; `estimate_gaps` was interrupted by AWS
(Fargate Spot) after 2 h 54 min, and the window was closed instead of waiting for a retry that starts from scratch.
`fact_consumption` and `run_cost` did not run, so the tables below were written from the Airflow task times and the
prices of `pipeline/costs.py`, not pasted from `run_cost`.

## Duration

| Task | Minutes | Result |
|---|---|---|
| `clean_readings` | 29.7 | success (all 112 blocks, from raw to clean) |
| `export_inventory` | < 1 | success |
| `estimate_gaps`, try 1 | 174.4 | stopped by AWS: `SpotInterruption` ("Your Spot Task was interrupted", exit code 137) |
| `estimate_gaps`, try 2 | 5 | started by Airflow's retry; stopped by hand to close the window |
| `fact_consumption`, `run_cost` | — | did not run |

## Resources

| Task | Runs on | vCPU | Memory (GB) |
|---|---|---|---|
| `clean_readings`, `estimate_gaps` | FARGATE_SPOT | 8 | 32 |
| `export_inventory` | Airflow worker | — | — |

Each Spark task: 100 GiB of scratch disk; Spark in local mode with 24 GB of driver memory (ADR-014). The full history
does not fit in memory: Spark spilled the cached readings to that disk, as planned.

## Cost

FARGATE_SPOT 8 vCPU and 32 GB: 8 × 0.0127 + 32 × 0.0014 = 0.1464 USD per hour.

| Task | USD |
|---|---|
| `clean_readings` | 0.07 |
| `estimate_gaps`, try 1 | 0.43 |
| `estimate_gaps`, try 2 | 0.01 |
| **Total Fargate** | **0.51** |

The monthly estimate (voltiagrid-docs, *Costs → Monthly estimate*) assumed this load once; even a complete run on
on-demand capacity (0.4517 USD per hour for the same size) stays a few dollars.

## The daily close on Fargate (KAN-275)

The same window ran the daily DAG `voltiagrid_readings` on ECS (run `f3-2026-11-04`, 3 dates): every Spark job as a
Fargate task (on-demand, 2 vCPU, 8 GB). `run_cost` reported **9.3 minutes** of wall clock (18.3 minutes of tasks,
part of them in parallel) and **0.0348 USD** of Fargate, well inside the 45 minutes of the daily close (US-55).

## What we learned

- Cleaning the full F1 history takes about 30 minutes on one 8 vCPU task.
- `estimate_gaps` is the long step: it had not finished after almost 3 hours. On Spot, a job that long is likely to
  be interrupted, and a retry starts again from zero.
- Next run (sprint 6): `estimate_gaps` and `fact_consumption` of the history on **on-demand** capacity, or
  `estimate_gaps` split by date range so that each task is short and a Spot interruption repeats only one range.

## Control counts between layers

Pending: `python -m jobs.layer_counts --run f1-full-2026-10-28` (KAN-278) needs the complete run. The clean layer of
the history is already in S3 (`clean/source=readings/dataset=interval_30min/`, run `f1-full-2026-10-28`).