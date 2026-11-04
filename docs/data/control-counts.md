# Control counts: a resend without losses or duplicates

US-31 (T-31.1), CA-08. Companion of *pipeline.md* (cleaning) and of `voltiagrid-api/docs/INGESTION.md`
(the consumer side).

## How it is checked

Three levels, from the rule to the real data:

| Level | What | Where |
|---|---|---|
| Rule | `pipeline.control_counts.reconcile(meters=1000, hours=24)`: simulates a resend that arrives in any order with 5% of the readings twice, cleans it with RN-02 and counts: expected 48,000, lost 0, duplicated 0 | `tests/rules/test_acceptance.py` (CA-08 enabled) |
| Spark jobs | 1,000 meters × 24 h written to raw as the consumer stores them (16 concentrators × 48 messages, 5% stored twice in another hour), read with `read_f3`, cleaned with `merge_and_write`: 48,000 clean rows, 0 lost, 0 duplicated; processing it again changes nothing | `tests/test_control_counts.py` |
| Real run | `jobs/control_counts.py` compares a raw partition with clean after the cleaning job | command below |

```powershell
python -m jobs.control_counts --ingest-date 2026-11-02 --lake ../voltiagrid-api/lake
```

`lost` = meter-intervals that have a reading in raw (clock within ±5 minutes) and no row in clean; `duplicated` = clean
rows beyond one per meter and interval. Both must be 0; the command exits with an error otherwise, so it can run as
the last task of the DAG. Readings with an invalid clock are counted apart (`raw_invalid_clock`).

Reference run (dev seed, 35 days of F3 = 6,720 messages): 328,598 readings in raw, 318,047 meter-intervals, 4,261 with an
invalid clock, 318,047 clean rows, **0 lost, 0 duplicated**, 64 s.
