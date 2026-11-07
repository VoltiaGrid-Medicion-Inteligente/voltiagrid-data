# Spark performance of the daily close (US-42)

RNF-02: the daily close must finish in less than 45 minutes at the lowest cost. This page is how the jobs are measured
(T-42.1) and what was changed (T-42.2); the report with the numbers is in voltiagrid-docs, *Performance → Spark*.

## 1. Measuring (T-42.1)

`jobs/benchmark.py` runs the daily close of one cleaning run in one Spark session, in the DAG's order, and records
for each job what Spark's own monitoring API (the web UI, `http://localhost:4040`) saw while it ran:

| Measure | What it tells |
|---|---|
| `seconds` | Wall clock of the job |
| `shuffle_read`, `shuffle_write` | Bytes moved between partitions: the expensive part (disk, network) |
| `stages`, `tasks` | How much work Spark split the job into |
| `joins` | Joins of each type in the executed plans: `BroadcastHashJoin` (the small side copied to every task, no shuffle) or `SortMergeJoin` (both sides shuffled and sorted) |

It also saves the physical plan of every query the job ran, `benchmark/<label>-plans/<job>.txt`. Every job overwrites
only its own partitions, so measuring does not change the data (CT-08).

```powershell
python -m jobs.benchmark --run f3-2026-11-04 --label before --lake ../voltiagrid-api/lake
# ... change the code ...
python -m jobs.benchmark --run f3-2026-11-04 --label after --lake ../voltiagrid-api/lake
python -m jobs.benchmark --compare benchmark/before.json benchmark/after.json
```

`benchmark/` is git-ignored: copy the tables it prints into the PR and the report. Measure before and after on the
same machine, the same data and with nothing else heavy running; write down the machine (cores, RAM) and the run.
A run of one day is the daily close; a run of many days (the first load of a laptop) stresses the jobs more.

## 2. What changed (T-42.2)

The measurement pointed at one job: `clean_readings` took more than half of the close. Its plan showed the raw JSON
scanned **twice**: once to find which dates the run touches and again to clean them. The changes, and why each one:

| Change | Where | Kind | Effect |
|---|---|---|---|
| Parse the raw files once (`localCheckpoint` of the raw readings before finding the dates) | `jobs/clean_readings.py` | Cache | The raw files are read and parsed once; the dates and the cleaning use the stored rows |
| `F.broadcast(meters)` in the joins with F4 | `circuit_load`, `fact_consumption`, `transformer_losses` | Broadcast join | The planner already broadcast F4 (a few hundred KB, under the 10 MB limit), as the plans show; the hint keeps it so if the inventory grows or its size is unknown |
| `SPARK_SHUFFLE_PARTITIONS` (default 8) | `jobs/spark.py` | Partitioning | 8 partitions fit a laptop and the 2-vCPU daily task; the F1 history on 8 vCPU should use about 4 per core (32). Adaptive execution merges small partitions, so a high value does not hurt small runs |

Things measured and **not** kept, because they did not make the close faster on this data:
caching the filled readings in `fact_consumption` (two reads of a small month) and filtering the weekday profile of
`estimate_gaps` to the filled days (the profile shuffle was already small after Spark's partial aggregation).

`tests/test_plans.py` checks that the joins with F4 stay `BroadcastHashJoin`.

**Reference measurement** (synthetic data: 1,000 meters, 35 days, 1.65 million readings; 2 cores, 8 shuffle
partitions; same machine before and after):

| Job | Seconds before → after | Shuffle (MB) before → after |
|---|---|---|
| `clean_readings` | 131.3 → 79.4 | 372.1 → 372.1 |
| `estimate_gaps` | 63.7 → 62.2 | 107.1 → 108.3 |
| `circuit_load` | 6.4 → 6.6 | 45.5 → 45.5 |
| `fact_consumption` | 7.7 → 7.1 | 0.9 → 0.9 |
| `billing` | 1.0 → 0.8 | 0.7 → 0.7 |
| **Total** | **210.1 → 156.1 (−26%)** | |

A one-day run (the daily close, 47,000 readings) went from 33.2 to 31.6 s on the same machine: at this size most of
the time is starting Spark and the small files, not the data. The report in voltiagrid-docs uses the team's own
measurement on the pilot lake.
