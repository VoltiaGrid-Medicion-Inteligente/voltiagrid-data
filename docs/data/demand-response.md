# Demand response (sprint 5)

US-43 declares the events, US-44 settles them and US-45 turns them into a fact table. The rules are plain Python in
`pipeline/demand_response.py` (tested by `tests/rules/`); the Spark jobs import the same functions and thresholds.

## 1. Declaring events (US-43, RN-08)

`jobs/dr_events.py`, for every circuit and day D:

| Input | Where it comes from | Why |
|---|---|---|
| Forecast temperature | Highest F2 temperature of D between 18:00 and 21:59 | The pilot replays history: the F2 value of the day stands in for the forecast |
| Projected load | Circuit load of the same peak intervals on D − 7 (`curated/dataset=circuit_load`, US-33) | Weekly persistence: the same weekday last week is the projection |

An event is declared when **both** values are strictly above their thresholds (30 °C and 85%). It starts at the first
peak interval whose projected load is above 85% (never before 18:00) and ends 3 hours later or at 22:00, whichever
comes first. One event per circuit and day at most.

```powershell
python -m jobs.circuit_load --date 2013-07-15 --lake ../voltiagrid-api/lake    # D - 7 must exist
python -m jobs.dr_events --date 2013-07-22 --lake ../voltiagrid-api/lake
```

Output: `curated/dataset=dr_events/date=YYYY-MM-DD/`

| Column | Meaning |
|---|---|
| `event_id` | `DR-<circuit>-<yyyymmdd>` |
| `circuit_code` | Circuit of the event |
| `start_ts`, `end_ts` | Event window (UTC, inside the peak band) |
| `duration_h` | Hours, 3 at most |
| `forecast_temp_c`, `projected_load_pct` | The two values that declared it |

A day without events keeps an empty partition with `_SUCCESS`, and `read_events` reads the dataset with an explicit
schema, so the next jobs never fail with `UNABLE_TO_INFER_SCHEMA` on a quiet day.
