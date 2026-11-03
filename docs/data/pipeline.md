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
