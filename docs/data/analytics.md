# Analytics layer: fact tables (sprint 5)

The fact tables of the dimensional model (voltiagrid-docs, *Data → Dimensional model*) built in the curated layer.
Power BI reads them in sprint 6, together with the dimensions of US-52.

## Keys

`jobs/analytics.py` makes the keys from the natural values, so no fact needs a lookup and a rerun gives the same keys:

| Key | Value | Example |
|---|---|---|
| `date_key` | date as an integer `yyyymmdd` | `20130115` |
| `time_key` | 30-minute slot of the day, 0 to 47 | 18:30 → `37` |
| `meter_id`, `customer_code`, `transformer_code`, `circuit_code` | F4 codes | `MAC000002`, `CU-MAC000002` |
| `band_name` | F7 band | `peak` |

`check_no_orphans` runs before writing: if a key of the fact has no row on its side (a customer or transformer that F4
does not have, an interval without an F7 band), the job stops with `orphan keys` and writes nothing.

Meters with readings that F4 does not have are **left out and counted** (`N meters without F4 left out`), as in the
monthly `fact_consumption` (US-35) and the control counts (US-41): without a customer and a transformer they cannot be
attributed. A local lake with the dev seed of F4 (200 households) and F1 readings of other households has many; the
full seed has every F1 household.

## 1. fact_consumption_interval (US-48)

One row per **meter** and **30-minute interval**.

```powershell
python -m jobs.fact_consumption_interval --date 2013-01-15 --lake ../voltiagrid-api/lake
python -m jobs.fact_consumption_interval --run f3-2026-11-02 --lake ../voltiagrid-api/lake
```

| Column | From |
|---|---|
| `date_key`, `time_key`, `interval_start` | Filled readings (US-34) |
| `meter_id`, `customer_code`, `transformer_code` | F4 |
| `band_name`, `tariff_valid_from` | F7, the band valid on the date (RN-05) |
| `energy_kwh` | Real or estimated reading |
| `energy_cost` | `energy_kwh × price_per_kwh` of the band, 4 decimals |
| `is_estimated` | 0 or 1 |
| `estimation_method` | `none`, `interpolation` or `profile` (RN-03) |

Output: `curated/dataset=fact_consumption_interval/date=YYYY-MM-DD/` (about 264,000 rows a day with the 5,500 pilot
meters). The monthly `fact_consumption` of US-35 stays: it is the billing aggregate.

Check: the kWh of the fact for a day equal the kWh of the filled readings of that day (same rows, no join loses one).

## 2. fact_transformer_loss_daily (US-51)

One row per **transformer** and **day**. Runs after `jobs.transformer_losses` and reads its two datasets.

```powershell
python -m jobs.fact_transformer_loss --date 2013-01-15 --lake ../voltiagrid-api/lake
```

| Column | From |
|---|---|
| `date_key`, `transformer_code` | `transformer_losses` |
| `circuit_code` | F4 |
| `delivered_kwh`, `metered_kwh`, `estimated_kwh`, `loss_kwh`, `loss_pct` | `transformer_losses` (RN-06) |
| `loss_cost` | Each half hour's loss (F5 − meters) at the F7 price of its band on that date, added per day (T-51.2) |
| `consecutive_days_over_12` | Days in a row up to this one with a loss above 12% (0 at or below 12%; a day without a value breaks the count). Looks back 31 days, like RN-06 |
| `is_suspicious`, `suspicious_rank` | `suspicious_transformers`: 1 and its rank of the day when the transformer is suspicious |

Output: `curated/dataset=fact_transformer_loss_daily/date=YYYY-MM-DD/`. `suspicious_transformers` is read with an
explicit schema, so a quiet day (no suspicious transformer, only `_SUCCESS`) does not stop the job.

### 2.1 Value of the loss (T-51.2)

A kWh lost at 19:00 is worth the peak price and one lost at 03:00 the off-peak price, so the job values every half
hour on its own: the same half hours RN-06 compares (present in F5 and in the meters), `delivered − metered` times the
`price_per_kwh` of the band valid on that date (`jobs.tariff_bands.assign_bands`, RN-05), added up per transformer and
day and rounded to 2 decimals. Example of the test: 0.3 kWh lost every half hour of a 2013 day is 14.4 kWh, worth
0.3 × (20 × 0.12 + 20 × 0.18 + 8 × 0.30) = **2.52**, not 14.4 times an average price.
