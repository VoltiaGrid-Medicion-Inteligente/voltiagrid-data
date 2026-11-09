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

**Status: completed on 9 November** (section *Second run* below). The first run, on 8 November, did not finish:
`clean_readings` processed the whole history; `estimate_gaps` was interrupted by AWS (Fargate Spot) after 2 h 54 min,
and the window was closed instead of waiting for a retry that starts from scratch. The tables of that run were written
from the Airflow task times and the prices of `pipeline/costs.py`, not pasted from `run_cost`.

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

## Second run, 9 November (KAN-356): complete

The history was cleaned again by `voltiagrid_history` (run `f1-dev-2026-10-28`: the `dev` parameter still processed
the 112 blocks, 167,817,209 readings in 829 dates, in 26 minutes). `estimate_gaps` on develop was still running after
an hour, so it was stopped and run again with the fix of `interpolate` (PR #76: the next valid reading is looked up in
one pass instead of once per row). Both remaining jobs ran as single ECS tasks, **on-demand**, 8 vCPU, 32 GB, 100 GiB
of scratch disk, image `voltiagrid/spark-jobs:a1665cc`:

| Task | Minutes | Result | USD |
|---|---|---|---|
| `estimate_gaps --run f1-dev-2026-10-28` | 24.7 | 174,362,736 rows in 829 dates: 167,811,186 real, 20,587 interpolated, 1,331,528 from the profile, 5,199,435 unavailable | 0.19 |
| `fact_consumption --run f1-dev-2026-10-28` | 6.1 | 16,089 rows, 200 customers, 28 months (2011-11 to 2014-02), 1,659,279.4 kWh | 0.05 |
| `layer_counts --run f1-full-2026-10-28` | 13.5 | control counts below | 0.11 |

Minutes are from the start to the stop of each Fargate task; USD = minutes × 0.4517 USD per hour (on-demand price of
that size).

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
- The real cause of the long `estimate_gaps` was the code, not the capacity: a window frame that ended at
  `unboundedFollowing` was recomputed for every row (about n²/2 per meter, with about 40,000 intervals per meter). With
  the fix (PR #76) the full history takes 24.7 minutes; the whole load (clean, estimate, consumption) is about one hour
  and under 1 USD of Fargate.
- The daily close of 9 November (`f3-2026-11-04`, 793,528 readings, on-demand 2 vCPU / 8 GB per job) took 12.2
  minutes of wall clock and 0.0358 USD (KAN-317).

## Control counts between layers

`python -m jobs.layer_counts --run f1-full-2026-10-28` as an ECS task on the S3 lake (KAN-357):

```
            raw_readings: 167,817,021
     raw_meter_intervals: 167,811,467
              clean_rows: 167,811,552
       lost_raw_to_clean: 0
     duplicated_in_clean: 0
             clean_valid: 167,811,186
             filled_rows: 174,362,736
             filled_real: 167,811,186
        filled_estimated: 6,551,550
 filled_month_rows_in_f4: 7,687,824
          fact_intervals: 7,687,824
  filled_month_kwh_in_f4: 1,659,279.3688
                fact_kwh: 1,659,279.3688
       meters_without_f4: 5,365
OK: raw, clean, filled and curated agree
```

With the dev seed of F4, only its 200 meters reach `fact_consumption`; the other F1 households are reported as
meters without F4 (KAN-298).