# Readings pipeline (sprint 4)

Spark jobs that take the readings from raw to clean and curated (ADR-002, ADR-003, ADR-007). Every job runs in
Spark local mode (`jobs/spark.py`): on a laptop, in CI and on Fargate (ADR-014). Paths follow the *Data lake
layout* page of voltiagrid-docs (`jobs/lake.py`).

## 1. Read raw with an explicit schema (US-30, T-30.1)

`jobs/readings.py` reads both sources into the same five columns, so the cleaning rules are written once.

| Column | Type | F1 (Kaggle CSV, `schemas/f1.py`) | F3 (concentrator JSON, `schemas/f3.py`) |
|---|---|---|---|
| `meter_id` | string | `LCLid` | `readings[].meter_id` |
| `original_ts` | timestamp_ntz, UTC | `tstp` | `readings[].interval_start` |
| `energy_text` | string, as sent | `energy(kWh/hh)` | `readings[].energy_kwh` |
| `seq` | long | 0 (F1 has no sequence) | `readings[].reading_seq` |
| `source` | string | `f1` | `f3` |

- Nothing is inferred. A CSV whose header does not match `F1_SCHEMA` fails (`enforceSchema=False`, `FAILFAST`).
- Energy stays as text: F3 sends a number, `null` or the text `"Null"`. A numeric schema would turn `"Null"` into
  null without a trace; the cleaning step decides and flags each case (T-30.4).
- F3 times carry a zone (`Z`); they are converted to UTC wall-clock time (`timestamp_ntz`), like F1.
- F3 files are read recursively under an `ingest_date=` folder, so every `ingest_hour=` inside it is included.

Locally the lake is the `lake/` folder of voltiagrid-api, where the F3 consumer writes (`--lake ../voltiagrid-api/lake`).

## 2. Normalize the clock, ±5 minutes (US-30, T-30.2, RN-01)

`jobs/clean_readings.py`, `normalize_clock`. Each reading moves to the nearest `:00` or `:30` if it is at most
5 minutes away (5:00 exactly still counts); otherwise it keeps `interval_start = null` and `clock_ok = false`.

| Reading at | `interval_start` | `clock_ok` |
|---|---|---|
| 10:33 | 10:30 | true |
| 10:27 | 10:30 | true |
| 10:35:00 | 10:30 | true |
| 10:35:01 | null | false |
| 10:41 | null | false (CA-01) |
| 23:57 | 00:00 of the next day | true |

The arithmetic is done on epoch seconds (`unix_timestamp`, session time zone UTC), so it is exact, crosses midnight
and month ends without special cases, and never depends on the machine's time zone.

## 3. Remove duplicates with a window (US-30, T-30.3, RN-02)

`deduplicate`, after `normalize_clock`, in two passes:

1. **Exact copies** (same meter, source, original time, sequence and value) become one row. They come from
   redeliveries: the consumer may store a message twice after a crash (ADR-007, `voltiagrid-api/docs/INGESTION.md`).
   This is what guarantees *no duplicates by meter + interval + sequence* (US-29).
2. **RN-02**: `row_number()` over a window partitioned by `meter_id, interval_start`, ordered by `seq` descending.
   Only row 1 stays. The highest sequence wins even if its value is null; the quality step flags it and the gap is
   estimated later (US-34). Ties (F1 has no sequence) go to the latest original time and then the highest value
   text, so the result never depends on how Spark split the data.

Readings with an invalid clock have no interval: they skip pass 2 and stay, flagged, for the quality report.

| Input | Kept |
|---|---|
| `MAC000002` 10:30, 0.20, seq 1 and 10:30, 0.30, seq 2 | 0.30, seq 2 (CA-02) |
| 10:28, seq 5 and 10:31, seq 3 (both normalize to 10:30) | seq 5 |
| 10:30, 0.30, seq 3 and 10:30, null, seq 5 | null, seq 5 |
| The same reading three times | one |

## 4. Quality flags and Parquet in clean (US-30, T-30.4)

`quality_flags` gives each reading one flag; the first that matches wins:

| Flag | When | Example |
|---|---|---|
| `invalid_clock` | more than 5 minutes from `:00` / `:30` (RN-01) | 10:41 |
| `null_value` | null, `"Null"`, empty | F1 `Null`, F3 `null` |
| `not_a_number` | text that is not a number | `abc` |
| `negative` | below 0 | -1.5 |
| `out_of_range` | above 25 kWh in 30 minutes (impossible for a household) | 31.2 |
| `ok` | everything else, including 0 (RN-07 looks for zeros) and 25 itself | 0.25 |

Flagged readings stay in clean with `is_valid = false` and `energy_kwh = null`: they are excluded from every
calculation and their interval is estimated like any gap (US-34). `energy_text` keeps what the source sent.

### Output

`clean/source=readings/dataset=interval_30min/date=YYYY-MM-DD/`, Parquet with Snappy, one folder per **reading date**
(Data lake layout), each with its `_SUCCESS` file.

| Column | Type | Meaning |
|---|---|---|
| `meter_id` | string | |
| `interval_start` | timestamp_ntz | normalized interval (null if invalid clock) |
| `energy_kwh` | double | null unless `is_valid` |
| `is_valid` | boolean | `quality_flag = 'ok'` |
| `quality_flag` | string | table above |
| `original_ts`, `energy_text`, `seq`, `source` | | as read from raw, so clean can be recomputed |
| `date` | date (partition) | date of `interval_start`, or of `original_ts` if the clock is invalid |

**Resends and reruns.** A resend brings old readings in today's raw partition. The job reads back what clean already
has for the dates it touches, runs the rules again on the union and overwrites only those dates
(`partitionOverwriteMode=dynamic`). A lower sequence never replaces a higher one, and running the same input twice
gives the same data (CT-08).

**Run manifest.** Each run writes `_runs/<run>.json` (folders starting with `_` are invisible to Spark readers) with the
dates it touched, the rows and the count per flag. The next jobs read the dates from it (`f3-2026-11-02`,
`f1-dev-local`, ...).

```powershell
python -m jobs.clean_readings --source f3 --ingest-date 2026-11-02 --lake ../voltiagrid-api/lake
python -m jobs.clean_readings --source f1 --input data/raw --mode dev --lake ../voltiagrid-api/lake
```

Reference run: the CA-08 resend of the simulator (784 messages, 47,906 readings) became 46,999 rows in 3 dates in
30 s on a laptop: 44,539 `ok`, 912 `null_value`, 631 `invalid_clock`, 477 `negative`, 440 `out_of_range`; the other
907 were duplicates.

**Windows.** Spark needs `winutils.exe` to write files on Windows. If a test or job fails with
`HADOOP_HOME and hadoop.home.dir are unset` or `UnsatisfiedLinkError ... NativeIO$Windows`, put `winutils.exe` and
`hadoop.dll` for Hadoop 3.4 in `C:\hadoop\bin`, run `setx HADOOP_HOME C:\hadoop`, add `C:\hadoop\bin` to the PATH and
open a new terminal. The CI runs on Linux and does not need it.

## Rules in `pipeline/` and their Spark twin

Each business rule has two implementations that must agree:

| Where | What for | Tested by |
|---|---|---|
| `pipeline/<rule>.py` | The rule in plain Python, readable next to the brief; the function each CA calls | `tests/rules/test_acceptance.py` |
| `jobs/<job>.py` | The same rule on millions of rows in Spark | `tests/test_<job>.py` |
| both | The job, run on the CA fixture, gives the rule's answer | `tests/rules/test_rules_match_spark.py` |

When a rule is implemented, its acceptance test stops being `xfail` and the **Status** of the traceability matrix
changes in the same PR.

| Rule | CA | `pipeline` | Spark job | Status |
|---|---|---|---|---|
| RN-01 clock ±5 min | CA-01 | `pipeline.cleaning.normalize_clock` | `jobs.clean_readings.normalize_clock` | Implemented (US-30) |
| RN-02 highest sequence | CA-02 | `pipeline.cleaning.deduplicate` | `jobs.clean_readings.deduplicate` | Implemented (US-30) |
