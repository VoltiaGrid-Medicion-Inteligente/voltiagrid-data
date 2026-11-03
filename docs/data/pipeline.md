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
