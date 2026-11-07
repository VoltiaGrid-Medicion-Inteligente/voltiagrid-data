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

### 1.1 Participants and notifications (T-43.2)

The customers of the event circuit whose contract valid on the event day has the **dynamic** tariff take part: they
are the time-of-use customers enrolled in the programme. A customer with the standard tariff is never called.

| Dataset | One row per | Columns |
|---|---|---|
| `curated/dataset=dr_participants/` | event and meter | `event_id`, `customer_code`, `meter_id`, `circuit_code`, `date` |
| `curated/dataset=dr_notifications/` | event and customer | `event_id`, `customer_code`, `channel` (`email`), `message`, `date` |

`dr_notifications` is an **outbox**: the message that a mailer (or SMS gateway) sends, kept in the lake so it can be
checked and resent. The pilot has no mail service, so sending is out of scope; the outbox is the evidence that each
participant was told the window in time.

```
Demand-response event on 2013-07-22 from 19:00 to 22:00: please reduce your consumption. Saving 10% or more against
your usual consumption at those hours earns an incentive.
```

### 1.2 CA-06 (T-43.3)

`pipeline.demand_response.declare_event(forecast_temp_c, projected_load_pct, day, overload_from)` is the rule as the
brief states it: the event (start, end and the two inputs) when both conditions hold, `None` otherwise. Two tests
check it:

| Test | What it proves |
|---|---|
| `tests/rules/test_acceptance.py::test_ca_06_event_only_when_both_conditions_and_max_3_hours` | CA-06: 31 °C and 86% declare; one condition alone does not; the event lasts 3 hours at most |
| `tests/rules/test_rules_match_spark.py::test_ca_06_spark_declares_the_same_events_as_the_rule` | `jobs.dr_events` declares exactly the same circuits and windows as the rule for 12 combinations |

## 2. Settling events (US-44, RN-09 and RN-10)

### 2.1 Baseline (T-44.1)

`jobs/dr_settlement.py`, for every event of the days and every participating customer:

1. **Baseline days** (`pipeline.demand_response.baseline_days`): the last 5 working days (Monday to Friday) before the
   event on which **that customer** had no event, looking back up to 4 weeks. The pilot has no holiday calendar, so a
   holiday counts as a working day (open point for the client).
2. **Baseline energy**: for each of those days, the kWh of the customer's meters in the same clock window as the event
   (19:00 to 22:00 for an event at those hours), from the filled readings (real and estimated, US-34: a gap must not look
   like a saving). The baseline is the average of the 5 days.
3. Fewer than 5 days with readings → `baseline_kwh` is null: there is no fair baseline and the customer is not settled.

```powershell
python -m jobs.dr_settlement --date 2013-07-22 --lake ../voltiagrid-api/lake
```

Output: `curated/dataset=dr_settlement/date=YYYY-MM-DD/`, one row per event and customer:
`event_id`, `customer_code`, `baseline_days`, `baseline_kwh`, `date`.

### 2.2 Saving and incentive (T-44.2)

| Column | How |
|---|---|
| `actual_kwh` | The customer's meters in the event window on the event day (filled readings) |
| `savings_kwh` | `baseline_kwh − actual_kwh` (negative when the customer consumed more than usual) |
| `savings_pct` | `savings_kwh / baseline_kwh`, rounded to 4 decimals |
| `incentive_paid` | `savings_pct >= 0.10` (RN-09: 8% gets nothing, 10% and 11% do) |
| `price_per_kwh` | F7 **peak** price valid on the event day (the window is always inside the peak band) |
| `incentive_amount` | `savings_kwh × price_per_kwh`, rounded to 2 decimals; 0 without incentive |

The rate (one peak price per kWh saved) is the pilot's assumption: the brief fixes when an incentive is paid, not how
much. If the client sets another rate, only `peak_price` in `jobs/dr_settlement.py` and `incentive_amount` in
`pipeline/demand_response.py` change.

### 2.3 Eight events a month at most (T-44.3, RN-10)

`jobs/dr_events.py` applies the limit when it picks the participants: a customer who already took part in
`MAX_EVENTS_PER_MONTH = 8` events in the calendar month (days before the event day) is left out and gets no
notification. The count reads every participation of the month in the lake and also the days processed earlier in the
same run, in date order, so running a whole month at once gives the same result as running it day by day. Events of
the previous month do not count.
