"""Suspicious meters (US-38, RN-07): zero streaks (T-38.1)."""

from datetime import datetime, timedelta

from jobs.suspicious_meters import evaluate_meters, zero_streaks

START = datetime(2013, 1, 15)
HALF_HOUR = timedelta(minutes=30)


def filled_frame(spark, by_meter):
    """{meter: [values]} from START every 30 minutes; a value 'E' is an estimate of 0 kWh, None a missing value."""
    rows = []
    for meter, values in by_meter.items():
        for i, v in enumerate(values):
            estimated = v == "E"
            rows.append((meter, START + i * HALF_HOUR, 0.0 if estimated else v, estimated))
    return spark.createDataFrame(rows, "meter_id string, interval_start timestamp_ntz, energy_kwh double, "
                                       "is_estimated boolean")


def streaks(spark, by_meter):
    return {r.meter_id: r.zero_streak_intervals for r in zero_streaks(filled_frame(spark, by_meter)).collect()}


def test_longest_run_of_real_zeros_is_counted(spark):
    assert streaks(spark, {"M1": [0.2] + [0.0] * 5 + [0.3] + [0.0] * 2}) == {"M1": 5}


def test_a_meter_without_zeros_has_a_streak_of_0(spark):
    assert streaks(spark, {"M1": [0.2, 0.3]}) == {"M1": 0}


def test_estimates_and_missing_values_break_a_run(spark):
    assert streaks(spark, {"M1": [0.0] * 3 + ["E"] + [0.0] * 2, "M2": [0.0] * 2 + [None] + [0.0]}) == {"M1": 3, "M2": 2}


def candidates(spark, meters):
    return spark.createDataFrame([(m, "TR-1", f"CU-{m}") for m in meters],
                                 "meter_id string, transformer_code string, customer_code string")


def test_48_hours_of_zeros_on_a_suspicious_transformer_is_a_suspicious_meter(spark):
    filled = filled_frame(spark, {"M1": [0.1] + [0.0] * 96, "M2": [0.1] + [0.0] * 95 + [0.1]})
    rows = {r.meter_id: r for r in evaluate_meters(candidates(spark, ["M1", "M2", "M3"]), filled).collect()}
    assert (rows["M1"].zero_streak_intervals, rows["M1"].is_suspicious) == (96, True)
    assert rows["M1"].reason == "zero consumption for 48 h or more"
    assert (rows["M2"].zero_streak_intervals, rows["M2"].is_suspicious) == (95, False)   # 47.5 hours
    assert (rows["M3"].zero_streak_intervals, rows["M3"].is_suspicious) == (0, False)    # no readings at all


# ---------- T-38.2: drop against the 8-week average, unless the contract changed ----------

DAY = datetime(2013, 3, 31).date()


def daily_frame(spark, by_meter):
    """{meter: (baseline kWh per day, last-week kWh per day)} -> filled readings, one interval per day is enough."""
    from jobs.suspicious_meters import periods

    (r_first, r_last), (b_first, _) = periods(DAY)
    rows = []
    for meter, (base, recent) in by_meter.items():
        d = b_first
        while d <= r_last:
            kwh = recent if d >= r_first else base
            rows.append((meter, datetime.combine(d, datetime.min.time()), kwh, False, d))
            d += timedelta(days=1)
    return spark.createDataFrame(rows, "meter_id string, interval_start timestamp_ntz, energy_kwh double, "
                                       "is_estimated boolean, date date")


def test_periods_are_the_last_7_days_and_the_8_weeks_before():
    from jobs.suspicious_meters import periods

    (r_first, r_last), (b_first, b_last) = periods(DAY)
    assert (str(r_first), str(r_last)) == ("2013-03-25", "2013-03-31")
    assert (str(b_first), str(b_last)) == ("2013-01-28", "2013-03-24")
    assert (b_last - b_first).days + 1 == 56


def test_drop_is_one_minus_last_week_over_the_8_week_average(spark):
    from jobs.suspicious_meters import consumption_drop

    rows = {r.meter_id: r for r in consumption_drop(daily_frame(spark, {"M1": (10.0, 3.0), "M2": (10.0, 5.0)}), DAY).collect()}
    assert (rows["M1"].avg_daily_kwh_8w, rows["M1"].avg_daily_kwh_last_week, rows["M1"].drop_pct) == (10.0, 3.0, 0.7)
    assert rows["M1"].baseline_days == 56
    assert rows["M2"].drop_pct == 0.5


def test_too_little_history_gives_no_drop(spark):
    from jobs.suspicious_meters import consumption_drop

    short = daily_frame(spark, {"M1": (10.0, 1.0)}).filter("date >= '2013-03-04'")  # 21 baseline days
    assert consumption_drop(short, DAY).first().drop_pct is None


def contracts_frame(spark, rows):
    from datetime import date

    return spark.createDataFrame([(c, f"CT-{c}", "standard", date.fromisoformat(s), date.fromisoformat(e) if e else None)
                                  for c, s, e in rows],
                                 "customer_code string, contract_code string, tariff_type string, start_date date, "
                                 "end_date date")


def test_drop_above_60_percent_is_suspicious_unless_the_contract_changed(spark):
    from jobs.suspicious_meters import consumption_drop, contract_changes

    filled = daily_frame(spark, {"M1": (10.0, 3.0), "M2": (10.0, 3.0), "M3": (10.0, 4.0), "M4": (10.0, 3.9)})
    contracts = contracts_frame(spark, [("CU-M1", "2011-11-01", None), ("CU-M2", "2011-11-01", "2013-03-10"),
                                        ("CU-M2", "2013-03-11", None)])
    result = evaluate_meters(candidates(spark, ["M1", "M2", "M3", "M4"]), filled,
                             consumption_drop(filled, DAY), contract_changes(contracts, DAY))
    rows = {r.meter_id: r for r in result.collect()}
    assert (rows["M1"].drop_60, rows["M1"].is_suspicious) == (True, True)
    assert rows["M1"].reason == "consumption more than 60% below its 8-week average"
    assert (rows["M2"].contract_changed, rows["M2"].is_suspicious) == (True, False)   # new contract on 2013-03-11
    assert rows["M3"].is_suspicious is False                                          # exactly 60%
    assert rows["M4"].is_suspicious is True                                           # 61%


def test_contract_changes_only_count_inside_the_window(spark):
    from jobs.suspicious_meters import contract_changes

    contracts = contracts_frame(spark, [("CU-OLD", "2012-01-01", None), ("CU-NEW", "2013-02-01", None),
                                        ("CU-END", "2011-01-01", "2013-03-30")])
    assert sorted(r.customer_code for r in contract_changes(contracts, DAY).collect()) == ["CU-END", "CU-NEW"]
