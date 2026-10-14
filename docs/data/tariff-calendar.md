# Tariff calendar (F7): structure and assumptions

US-04 (KAN-78), T-04.2 (KAN-89). The calendar is `data/reference/tariffs.csv`, its schema is `schemas/f7.py` and its rules are checked by `validate_tariffs.py` and `tests/test_tariffs.py`.

## What the brief asks for

- The regulator approved an hourly tariff with three bands: **peak**, **intermediate** and **off-peak** (punta, intermedia, valle).
- F7 is a flat CSV with the bands and the price per kWh valid on each date, fewer than 100 rows, updated quarterly.
- RN-05: the consumption of each interval is assigned to the band valid on the reading date.

## File structure

| Column          | Type (`schemas/f7.py`) | Meaning                                                                                         |
| --------------- | ---------------------- | ----------------------------------------------------------------------------------------------- |
| `valid_from`    | date                   | First day the row applies (inclusive)                                                           |
| `valid_to`      | date                   | Last day the row applies (inclusive)                                                            |
| `start_time`    | text `HH:MM:SS`        | First second of the band in the day (inclusive)                                                 |
| `end_time`      | text `HH:MM:SS`        | Last second of the band in the day (inclusive)                                                  |
| `band_name`     | text                   | `peak`, `intermediate` or `off-peak` (same names as `dim_tariff_band` in the dimensional model) |
| `price_per_kwh` | decimal(10,4)          | Price of one kWh in that band                                                                   |

Times are text because Spark has no time-of-day type. A validity period is the set of rows that share the same `valid_from` and `valid_to`.

## Current calendar

| Period                   | Off-peak                                  | Intermediate           | Peak                   |
| ------------------------ | ----------------------------------------- | ---------------------- | ---------------------- |
| 2011-01-01 to 2011-12-31 | 00:00-07:59:59 and 22:00-23:59:59 at 0.10 | 08:00-17:59:59 at 0.15 | 18:00-21:59:59 at 0.25 |
| 2012-01-01 to 2099-12-31 | 00:00-07:59:59 and 22:00-23:59:59 at 0.12 | 08:00-17:59:59 at 0.18 | 18:00-21:59:59 at 0.30 |

8 rows, 2 validity periods.

## Assumptions

1. **Prices are illustrative.** The brief gives no tariff values or currency. The prices are made up to exercise the rules, and we read them as **USD per kWh**, the currency the brief uses for its cost estimates. Taxes and fixed charges are not included. _To confirm with the product owner._
2. **Band hours.** Peak is the evening household peak (18:00-22:00), intermediate covers the working day (08:00-18:00) and off-peak the night (22:00-08:00). This matches the shape of the F1 consumption profile, where the highest load is in the early evening.
3. **Same bands every day.** There is no difference between weekdays, weekends or holidays, because the brief does not ask for it. If needed, a `day_type` column can be added without changing the other columns.
4. **Two periods.** 2011 covers the start of the F1 history (data begins in November 2011). From 2012 prices go up by about 20% so the calendar has a real change of price on a known date, which is what RN-05 must handle. The last period ends on 2099-12-31 to mean "valid until a new period is added".
5. **Local time, no time zone.** Band hours are London local clock time, the same as the F1 timestamps (`tstp` is loaded without time zone). Daylight saving changes are not adjusted.
6. **Inclusive limits.** Both dates and both times are inclusive, and consecutive bands are exactly one second apart (07:59:59 then 08:00:00). Readings start on :00 or :30, so every reading falls inside one band.
7. **All customers use this calendar.** F1 tells whether a household was on a standard or a dynamic tariff in the London trial (`stdorToU`). That describes the source data, not Voltia's tariff: billing applies this calendar to every pilot customer.

## Rules that keep it unambiguous

`validate_tariffs.py` (exit code 1 on failure) and `tests/test_tariffs.py` check that:

- there are fewer than 100 rows and at least 2 validity periods;
- the three bands exist and every price is greater than 0;
- `valid_from` is not after `valid_to`, and periods neither overlap nor leave a gap between them;
- inside each period the bands start at 00:00:00, end at 23:59:59 and leave no gap or overlap;
- every half-hour from November 2011 to March 2014 (the F1 and F2 period) gets exactly one band.

`band_for(rows, timestamp)` returns the single row that applies to a timestamp and fails if there are zero or more than one.

## How to add a new period (quarterly update)

1. Change the `valid_to` of the current last period to the day before the new prices start.
2. Add the rows of the new period with that start date and `valid_to` 2099-12-31.
3. Never edit the prices of past periods: bills already issued depend on them.
4. Run `python validate_tariffs.py` and `python -m pytest tests/test_tariffs.py`, then open a PR.
