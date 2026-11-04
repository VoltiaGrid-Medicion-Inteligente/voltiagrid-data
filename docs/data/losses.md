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
