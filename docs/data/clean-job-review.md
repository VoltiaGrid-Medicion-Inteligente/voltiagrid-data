# Review of the cleaning job (US-30, T-30.6)

Reviewer: data lead. Job: `jobs/clean_readings.py` (T-30.1 to T-30.5). Method: read the code against the brief
(RN-01, RN-02, US-30 acceptance criteria) and the *Data lake layout* page, run it on the simulator resend and on the
F1 development subset, and check the output with `jobs/clean_report.py`.

## Checklist

| # | Check | Result | Evidence |
|---|---|---|---|
| 1 | Raw is read with an explicit schema; a different header fails | OK | `schemas/f1.py`, `schemas/f3.py`, `test_f1_file_with_other_columns_fails_instead_of_being_guessed` |
| 2 | 10:33 → 10:30; 10:41 → invalid clock (RN-01, CA-01) | OK | `test_clock_is_normalized_within_5_minutes_or_flagged`, CA-01 enabled |
| 3 | Duplicate interval keeps the highest sequence (RN-02, CA-02) | OK | `test_ca_02_same_interval_keeps_the_highest_sequence`, CA-02 enabled |
| 4 | Null, negative and > 25 kWh are flagged and excluded | OK | `test_each_defect_gets_its_flag_and_no_energy` |
| 5 | Parquet (Snappy) in clean, partitioned by reading date, `_SUCCESS` per partition | OK | `test_job_writes_parquet_partitioned_by_date_with_success_markers` |
| 6 | A resend does not delete what clean already had; rerun gives the same data (CT-08) | OK | `test_a_resend_is_merged_with_what_clean_already_had`, `test_running_the_same_input_twice_gives_the_same_clean_data` |
| 7 | No duplicates by meter + interval after a run | OK | `jobs.clean_report`: 0 on the resend below |
| 8 | Same rule in `pipeline/` and in Spark | OK | `tests/rules/test_rules_match_spark.py` |
| 9 | Nothing depends on the machine's time zone | OK | session time zone UTC (`jobs/spark.py`), times as `timestamp_ntz` |
| 10 | No credentials or absolute paths in the code | OK | lake paths come from `--lake` / `--raw` / `--clean` |

## Runs

### F3: CA-08 resend from the simulator

1,000 meters, 24-hour outage (`voltiagrid-api/docs/INGESTION.md`), 784 messages and 47,906 readings in raw.

```powershell
python -m jobs.clean_readings --source f3 --ingest-date <date> --lake ../voltiagrid-api/lake
python -m jobs.clean_report --lake ../voltiagrid-api/lake
```

| Source | Flag | Rows | Share |
|---|---|---|---|
| f3 | `ok` | 44,539 | 94.77% |
| f3 | `null_value` | 912 | 1.94% |
| f3 | `invalid_clock` | 631 | 1.34% |
| f3 | `negative` | 477 | 1.01% |
| f3 | `out_of_range` | 440 | 0.94% |

46,999 rows in 3 dates (2012-12-31 to 2013-01-02), 999 meters, 0 duplicates, 30 s on a laptop. The 907 readings
that disappeared were duplicates (the simulator injected 925 at a 2% rate; a few of them also had an invalid clock,
so they stay as separate flagged rows). The shares match `DEFECT_RATE=0.02`: nulls about 2%, out of range (negative
plus above 25) about 2%.

### F1: development subset (4 blocks, 200 households)

```powershell
python -m jobs.clean_readings --source f1 --input data/raw --mode dev --lake ../voltiagrid-api/lake
python -m jobs.clean_report --lake ../voltiagrid-api/lake
```

| Source | Flag | Rows | Share |
|---|---|---|---|
| f1 | `ok` | 5,951,439 | 100.00% |
| f1 | `invalid_clock` | 199 | 0.00% |


| Check | Value |
|---|---|
| Rows | 5,951,638 |
| Valid readings | 100.00% |
| Duplicates (meter + interval) | 0 |
| Dates | 829 (2011-11-23 to 2014-02-28) |
| Meters | 200 |
| Run time and laptop | 267.1 s|

## Findings and decisions

1. **F1 has no sequence.** Two F1 rows for the same meter and interval tie at `seq = 0`; the latest original time and
   then the highest value text win. Deterministic, documented in `deduplicate`. F1 duplicates are rare (US-03 profile).
2. **Invalid-clock readings are dated by their original time.** They have no interval, so the partition is the date of
   `original_ts`. They never enter calculations (`is_valid = false`).
3. **Small files.** One partition per reading date: about 264,000 rows a day for the pilot, a few MB of Parquet, below
   the 64-256 MB target of the layout. Accepted for the pilot (fewer, bigger files would mean monthly partitions and
   rewriting a month for each resend). To revisit at 200,000 meters (US-58), when a day is about 9.6 million rows.
4. **Merging reads back the touched dates.** A resend touching many dates reads all of them again. For the full F1
   history (CA-09, 167 million rows) run it block group by block group or on a machine with enough memory: the merged
   result is materialized (`localCheckpoint`) before overwriting.
5. **Windows needs `winutils.exe`** to write files with Spark (see *pipeline.md*). CI (Linux) does not.

Verdict: **approved**. US-30 acceptance criteria are met; findings 3 and 4 are recorded for US-41 and US-58.
