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

## 5. Network inventory from F4 (US-33, T-33.2)

The inventory (meters, customers, transformers, circuits, contracts) lives in the API database. `jobs/inventory.py`
copies it to raw once, and every job reads those files with an explicit schema (`schemas/f4.py`) instead of querying
the database from Spark (no JDBC driver, no load on the operational database).

```powershell
# voltiagrid-api stack up (make up) so the database has the seed; DATABASE_URL as in voltiagrid-api/.env
$env:DATABASE_URL = "postgresql+psycopg://voltia:<password>@localhost:5433/voltiagrid"
python -m jobs.inventory export --lake ../voltiagrid-api/lake
```

| Dataset (raw) | File | Columns |
|---|---|---|
| `source=f4/dataset=meters/ingest_date=YYYY-MM-DD/` | `meters.csv` | `meter_id`, `customer_code`, `transformer_code`, `transformer_capacity_kva`, `circuit_code`, `circuit_capacity_kva` |
| `source=f4/dataset=contracts/ingest_date=YYYY-MM-DD/` | `contracts.csv` | `customer_code`, `contract_code`, `tariff_type`, `start_date`, `end_date` |

Each partition has its `_manifest.json`. `read_meters` and `read_contracts` take the latest `ingest_date` unless one is
given. `circuit_capacity` returns one row per circuit with `capacity_kva` (kVA), and fails if F4 gives a circuit two
capacities.

With the dev seed (`MODE=dev`): 200 meters, 4 transformers, 1 circuit of 2,100.12 kVA.

## 6. Circuit load as a share of capacity (US-33, T-33.1)

`jobs/circuit_load.py`. For each circuit and 30-minute interval:

```
energy_kwh = sum of the valid clean readings of the circuit's meters (F4: meter -> transformer -> circuit)
load_kw    = energy_kwh x 2                     (average power over 30 minutes)
load_pct   = load_kw / capacity_kva             (capacity from F4; power factor 1)
is_overloaded = load_pct > 0.85                 (exactly 85% is not overloaded; same threshold as RN-08)
```

Output: `curated/dataset=circuit_load/date=YYYY-MM-DD/` with `circuit_code`, `interval_start`, `energy_kwh`,
`load_kw`, `capacity_kva`, `load_pct`, `is_overloaded`, `meters_reporting`, `meters_total`.
`meters_reporting` below `meters_total` means some readings were missing or invalid in that interval, so the load is
a lower bound until the gaps are estimated (US-34).

Every job after cleaning chooses its dates the same way (`jobs/lake.py`): `--date YYYY-MM-DD` (repeatable) or
`--run <run>`, the dates a cleaning run touched.

```powershell
python -m jobs.circuit_load --run f3-2026-11-02 --lake ../voltiagrid-api/lake
```

Reference run (dev seed, 35 days of simulated F3): 1,680 circuit-intervals in 36 dates, peak 7.0% of the 2,100 kVA
circuit, none above 85%. With 200 households on a circuit sized for thousands, overload only appears in the tests.

Tests (`tests/test_circuit_load.py`, T-33.3): 85.00% is not overloaded and 85.02% is; invalid readings add no load;
meters missing from F4 are left out; one row per circuit and interval; and the whole job reads the dates of a
cleaning run, joins F4 and writes `curated/dataset=circuit_load` with `_SUCCESS` per date.

## 7. Estimate gaps (US-34, RN-03)

`jobs/estimate_gaps.py` writes `clean/source=readings/dataset=interval_30min_filled/date=YYYY-MM-DD/`: exactly
**48 rows per meter and day**, the valid reading when there is one and an estimate otherwise.

### 7.1 Full interval grid (T-34.1)

A gap is an interval with no row at all (the meter did not report) or with a flagged reading (null, negative, out of
range). To find them, `complete_grid` builds every interval of every meter (`sequence` of 30-minute steps,
`explode`) and left-joins the clean readings:

| Column | Meaning |
|---|---|
| `actual_kwh` | the valid reading, or null (gap) |
| `quality_flag` | the clean flag, or `missing` when the meter sent nothing for that interval |

A meter's grid starts on the first day it has a reading with a valid clock, so a new meter is not estimated for days
before it existed. A meter that sent nothing on a day still gets its 48 intervals (a missing day, defect of US-02).

The job reads clean from 28 days before the first date (the profile of T-34.3 needs them) to the day after the last.

### 7.2 Linear interpolation, gaps up to 2 hours (T-34.2)

`interpolate`: a gap of **up to 4 intervals** (2 hours) with a valid reading on both sides is filled on the straight
line between them, rounded to 4 decimals, with `is_estimated = true` and `estimation_method = 'interpolation'`.

How it is found, with windows per meter ordered by time (gaps and islands):

```
_pos       = row_number()                                 position in the grid
_prev_pos  = last(position of a valid reading) up to here     _prev_kwh = its value
_next_pos  = first(position of a valid reading) from here     _next_kwh = its value
gap length = _next_pos - _prev_pos - 1
estimate   = _prev_kwh + (_next_kwh - _prev_kwh) x (_pos - _prev_pos) / (_next_pos - _prev_pos)
```

Example: 0.2 at 4:30, nothing at 5:00, 5:30 and 6:00, 0.6 at 6:30 → 0.3, 0.4, 0.5. The grid includes the day before and
the day after, so a gap that crosses midnight is measured and interpolated with the readings on both sides. A gap
longer than 2 hours, or with no valid reading on one side (the end of the data), is left for the profile (T-34.3).
