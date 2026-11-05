# Losses: transformers and meters (sprint 4)

Continues *pipeline.md* (cleaning, estimation, consumption and billing) with the loss stories, US-37 and
US-38. Kept in its own page so this work and the billing work can advance at the same time.

## 1. Transformer losses (US-37, RN-06)

### 1.1 F5 to raw and 15 → 30 minute alignment (T-37.1)

`jobs/fetch_f5.py` asks the F5 API (voltiagrid-api, `api-transformer-metering`, port 8005) for the energy delivered to
every transformer during one day and stores each answer as received:

```
raw/source=f5/dataset=transformer_energy/ingest_date=<today>/TR-00001_20130105T0000Z_20130106T0000Z.json
```

The name has no `:` so it works on Windows. `jobs/transformer_losses.py` reads those files with an explicit schema
(`schemas/f5.py`); a day fetched twice counts once.

F5 measures every **15 minutes** and the meters every **30**. `to_half_hour` floors each quarter to its half hour
(`:15 → :00`, `:45 → :30`, epoch arithmetic as in RN-01) and adds the two quarters. A half hour with only one quarter is
dropped: comparing half the delivered energy with 30 minutes of meters would invent a loss.

```powershell
docker compose up -d api-transformer-metering        # in voltiagrid-api
python -m jobs.fetch_f5 --date 2013-01-05 --lake ../voltiagrid-api/lake
```

`jobs/inventory.py` and `jobs/fetch_f5.py` now share `jobs.lake.put_object` to write a raw object locally or to S3.

### 1.2 Daily loss per transformer (T-37.2)

```
metered_kwh = sum of the transformer's meters (F4) in the half hour, real and estimated (filled readings, US-34)
loss_kwh    = delivered_kwh (F5) - metered_kwh          over the half hours present on both sides
loss_pct    = loss_kwh / delivered_kwh                  per transformer and day
```

The meters side uses the **filled** readings: with only real ones, every missing reading would look like stolen energy.
`estimated_kwh` shows how much of `metered_kwh` was estimated. In the simulator, F5 delivers the meters' energy ×
(1 + injected loss), so an injected 15% shows as `loss_pct` 0.1304 (15 / 115).

Output: `curated/dataset=transformer_losses/date=YYYY-MM-DD/` with `transformer_code`, `delivered_kwh`, `metered_kwh`,
`loss_kwh`, `loss_pct`, `estimated_kwh`, `intervals`, `meters`.

```powershell
python -m jobs.fetch_f5 --date 2013-01-05 --lake ../voltiagrid-api/lake
python -m jobs.transformer_losses --date 2013-01-05 --lake ../voltiagrid-api/lake
```

Reference run (dev seed, 2013-01-05, no injected loss): the 4 transformers between -5.0% and 2.4%. Without injected
losses the difference comes only from the defects of F3 (duplicates with another value, estimates), far below the 12%
of RN-06.

### 1.3 Three consecutive days above 12% (T-37.3)

`suspicious_transformers`: a transformer is **suspicious on a day** when its `loss_pct` was above 0.12 that day and the
two days before (exactly 12% does not count; a day without a loss value breaks the streak). Gaps and islands again:
for the days above the threshold, `date - row_number()` is equal for consecutive days and names each streak;
`streak_days` and `streak_loss_kwh` accumulate along it. The suspicious transformers of each day are ranked by
`streak_loss_kwh` (1 = most energy lost).

Output: `curated/dataset=suspicious_transformers/date=YYYY-MM-DD/` with `transformer_code`, `rank`, `streak_days`,
`streak_start`, `streak_loss_kwh`, `loss_pct`, `loss_kwh`, `delivered_kwh`, `metered_kwh`. The job reads up to 31 days of
daily losses before the dates it processes, so a streak that started earlier is still counted.

Reference run (dev seed, F5 with a 15% loss injected on `TR-00002` from 2013-01-15 to 2013-01-18):

```powershell
Invoke-RestMethod -Method Post http://localhost:8005/losses -ContentType application/json `
  -Body '{"transformer_code":"TR-00002","start":"2013-01-15T00:00:00Z","end":"2013-01-19T00:00:00Z","loss_pct":0.15}'
python -m jobs.fetch_f5 --date 2013-01-14 --date 2013-01-15 --date 2013-01-16 --date 2013-01-17 --date 2013-01-18 --date 2013-01-19 --lake ../voltiagrid-api/lake
python -m jobs.transformer_losses --date 2013-01-14 --date 2013-01-15 --date 2013-01-16 --date 2013-01-17 --date 2013-01-18 --date 2013-01-19 --lake ../voltiagrid-api/lake
```

| Day | `TR-00002` loss | Result |
|---|---|---|
| 01-14 | 0.4% | |
| 01-15, 01-16 | 13.2%, 13.1% | streak of 1 and 2 days |
| 01-17 | 13.1% | **suspicious**, #1, 3 days, 119.1 kWh lost |
| 01-18 | 13.2% | **suspicious**, #1, 4 days, 159.0 kWh lost |
| 01-19 | -0.5% | streak over |

The same 15% injected in the first week of January (`TR-00001`, 01-06 to 01-09) gave 12.0%, 14.2%, 5.8% and 13.1%, so
no 3-day streak. Two simulator effects explain it: a duplicate of an out-of-range reading multiplied by 0.5-1.5 can
land below 25 kWh and pass as valid (one 17.7 kWh reading on 01-06), and in the first week the profile estimates have
only one previous week (01-01, a holiday) to learn from. Both shrink after a few weeks of history.

## 2. Suspicious meters (US-38, RN-07)

`jobs/suspicious_meters.py` runs after `jobs.transformer_losses`. For each transformer that is suspicious on the day
(RN-06), it evaluates the transformer's meters (F4) and writes one row per meter to
`curated/dataset=suspicious_meters/date=YYYY-MM-DD/` with `is_suspicious` and `reason`.

### 2.1 Zero for 48 hours (T-38.1)

`zero_streaks`: the longest run of consecutive **real** readings of 0 kWh per meter, over the day and the two days
before (room for a 48-hour run that reaches the day). An estimate or a missing value breaks the run: an estimate is not
evidence that the meter read zero. Gaps and islands: `row_number()` over all intervals minus `row_number()` over the
intervals in the same zero / non-zero state is constant inside each run.

`zero_streak_intervals >= 96` (48 hours of 30-minute readings) → `zero_48h = true`, suspicious, reason
`zero consumption for 48 h or more`. 95 intervals (47.5 hours) is not enough.

```powershell
python -m jobs.suspicious_meters --date 2013-01-17 --lake ../voltiagrid-api/lake
```

Reference run (the two suspicious days of `TR-00002` above): 100 meter-days evaluated (50 meters × 2), none with 48
hours of zeros. The simulator never sends long runs of zeros; the tests build them.

### 2.2 Drop against the 8-week average (T-38.2)

```
recent   = the day and the 6 before                 (7 days)
baseline = the 8 weeks just before the recent days  (56 days)
drop_pct = 1 - avg daily kWh (recent) / avg daily kWh (baseline)
drop_60  = drop_pct > 0.60 and no contract change   (exactly 60% is not a drop above 60%)
```

- Daily kWh uses the filled readings (real and estimated): a meter with gaps is not a meter that dropped.
- `drop_pct` stays null when the baseline has fewer than **28 days** of data or is 0: there is no fair average to compare
  with (a meter installed last month).
- **Contract change** (`contract_changes`): the customer has a contract (F4, `contracts.csv`) that started or ended
  between the first day of the baseline and the day. A new holder, a new tariff or a move explains a drop, so the meter is
  not suspicious for it.

A meter is suspicious when `zero_48h` or `drop_60` is true; `reason` lists which (`zero consumption for 48 h or more`,
`consumption more than 60% below its 8-week average`, or both separated by `;`). Every evaluated meter is written, with
`zero_streak_intervals`, `avg_daily_kwh_8w`, `baseline_days`, `avg_daily_kwh_last_week`, `drop_pct` and
`contract_changed`, so the control center can see why a meter was or was not flagged.

Example (day 2013-03-31): baseline 10 kWh/day, last week 3 kWh/day → `drop_pct` 0.70, suspicious; the same drop with a
contract that started on 2013-03-11 → not suspicious.

### 2.3 Tests (T-38.3)

`tests/test_suspicious_meters.py` covers each piece (zero runs, estimates breaking a run, 95 vs 96 intervals, periods,
drop, 28-day minimum, 60% vs 61%, contract changes inside and outside the window) and the whole job on a small lake:
a suspicious transformer with four meters (48 h of zeros, an 80% drop, the same drop with a new contract, a normal meter)
and a normal transformer whose meter reads zero. Only the four meters of the suspicious transformer are evaluated;
the first two are flagged with their reason, the third is not because its contract changed, and running the job twice
gives the same rows.
