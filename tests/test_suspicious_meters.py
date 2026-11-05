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


# ---------- T-38.3: the whole job on a small lake ----------

def build_lake(spark, tmp_path):
    """TR-S is suspicious on DAY, TR-OK is not. Meters:
    Z  on TR-S: zero for the last 3 days                 -> suspicious (zero 48 h; its weekly drop is only 43%)
    D  on TR-S: 10 kWh/day, then 2 kWh/day last week     -> suspicious (drop 80%)
    C  on TR-S: same drop but a new contract             -> not suspicious
    N  on TR-S: normal                                   -> evaluated, not suspicious
    X  on TR-OK: zero for 3 days                         -> not evaluated (its transformer is not suspicious)
    """
    from datetime import date
    from decimal import Decimal

    from jobs.estimate_gaps import filled_path
    from jobs.inventory import write_raw
    from jobs.lake import Lake
    from jobs.suspicious_meters import periods
    from jobs.transformer_losses import suspicious_path

    lake = Lake.local(str(tmp_path / "lake"))
    tx = {"Z": "TR-S", "D": "TR-S", "C": "TR-S", "N": "TR-S", "X": "TR-OK"}
    write_raw(lake, {
        "meters": (["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva", "circuit_code",
                    "circuit_capacity_kva"],
                   [(m, f"CU-{m}", t, Decimal("100"), "CT-1", Decimal("1000")) for m, t in tx.items()]),
        "contracts": (["customer_code", "contract_code", "tariff_type", "start_date", "end_date"],
                      [(f"CU-{m}", f"CT-{m}-1", "standard", date(2011, 11, 1), None) for m in tx]
                      + [("CU-C", "CT-C-2", "dynamic", date(2013, 3, 20), None)]),
    }, date(2026, 11, 1))

    (r_first, _), (b_first, _) = periods(DAY)
    zero_from = DAY - timedelta(days=2)
    rows, d = [], b_first
    while d <= DAY:
        for i in range(48):
            ts = datetime.combine(d, datetime.min.time()) + i * HALF_HOUR
            for m in tx:
                if m in ("Z", "X") and d >= zero_from:
                    kwh = 0.0
                elif m in ("D", "C") and d >= r_first:
                    kwh = 2.0 / 48
                else:
                    kwh = 10.0 / 48
                rows.append((m, ts, kwh, False, "ok", d))
        d += timedelta(days=1)
    spark.createDataFrame(rows, "meter_id string, interval_start timestamp_ntz, energy_kwh double, is_estimated boolean, "
                                "quality_flag string, date date") \
        .write.partitionBy("date").parquet(filled_path(lake))
    spark.createDataFrame([("TR-S", 1, 3, DAY)], "transformer_code string, rank int, streak_days int, date date") \
        .write.partitionBy("date").parquet(suspicious_path(lake))
    return lake


def test_job_flags_the_right_meters_with_their_reasons(spark, tmp_path):
    from jobs.suspicious_meters import run

    lake = build_lake(spark, tmp_path)
    rows = {r.meter_id: r for r in run(spark, lake, [DAY]).collect()}
    assert set(rows) == {"Z", "D", "C", "N"}                       # X is on a transformer that is not suspicious
    assert rows["Z"].is_suspicious and rows["Z"].zero_streak_intervals == 144
    assert rows["Z"].reason == "zero consumption for 48 h or more"
    assert rows["Z"].drop_pct == 0.4286                            # (4 x 10 + 3 x 0) / 7 = 5.71 of 10
    assert rows["D"].is_suspicious and rows["D"].drop_pct == 0.8
    assert (rows["C"].contract_changed, rows["C"].is_suspicious) == (True, False)
    assert rows["N"].is_suspicious is False and rows["N"].reason is None


def test_running_the_job_twice_gives_the_same_rows(spark, tmp_path):
    from jobs.suspicious_meters import run

    lake = build_lake(spark, tmp_path)
    run(spark, lake, [DAY])
    first = sorted((r.meter_id, r.is_suspicious) for r in spark.read.parquet(lake.curated("suspicious_meters")).collect())
    run(spark, lake, [DAY])
    second = sorted((r.meter_id, r.is_suspicious) for r in spark.read.parquet(lake.curated("suspicious_meters")).collect())
    assert first == second and len(first) == 4
