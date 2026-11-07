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
