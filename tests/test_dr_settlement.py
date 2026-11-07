"""Settlement of demand-response events (US-44): baseline (T-44.1), saving and incentive (T-44.2)."""

from datetime import date, datetime, timedelta

from jobs.lake import Lake

DAY = date(2013, 7, 22)                     # Monday; the event is 19:00-22:00 (6 intervals)
EVENT = "DR-CT-1-20130722"
HALF_HOUR = timedelta(minutes=30)


def build_lake(spark, tmp_path, kwh=None, missing=()):
    """M1 (CU-1), M3 and M4 (CU-3), M9 (CU-9). CU-1 also took part in an event on the 17th.
    Every interval is 1 kWh, except kwh[(meter, day)] for the whole day and 5 kWh at 18:30 (outside the window).
    missing: (meter, day) without readings."""
    from jobs.dr_events import EVENT_SCHEMA, PARTICIPANT_SCHEMA
    from jobs.estimate_gaps import filled_path

    kwh = kwh or {}
    lake = Lake.local(str(tmp_path / "lake"))
    events = [(EVENT, "CT-1", datetime(2013, 7, 22, 19), datetime(2013, 7, 22, 22), 3.0, 31.0, 0.9, DAY),
              ("DR-CT-1-20130717", "CT-1", datetime(2013, 7, 17, 18), datetime(2013, 7, 17, 21), 3.0, 31.0, 0.9,
               date(2013, 7, 17))]
    spark.createDataFrame(events, EVENT_SCHEMA).write.partitionBy("date").parquet(lake.curated("dr_events"))
    parts = [(EVENT, "CU-1", "M1", "CT-1", DAY), (EVENT, "CU-3", "M3", "CT-1", DAY), (EVENT, "CU-3", "M4", "CT-1", DAY),
             (EVENT, "CU-9", "M9", "CT-1", DAY), ("DR-CT-1-20130717", "CU-1", "M1", "CT-1", date(2013, 7, 17))]
    spark.createDataFrame(parts, PARTICIPANT_SCHEMA).write.partitionBy("date").parquet(lake.curated("dr_participants"))
    rows, d = [], date(2013, 7, 8)
    while d <= DAY:
        for meter in ("M1", "M3", "M4", "M9"):
            if (meter, d) in missing:
                continue
            ts = datetime.combine(d, datetime.min.time())
            for _ in range(48):
                value = 5.0 if ts.strftime("%H:%M") == "18:30" else kwh.get((meter, d), 1.0)
                rows.append((meter, ts, value, False, None, "ok", d))
                ts += HALF_HOUR
        d += timedelta(days=1)
    spark.createDataFrame(rows, "meter_id string, interval_start timestamp_ntz, energy_kwh double, is_estimated boolean, "
                                "estimation_method string, quality_flag string, date date") \
        .write.partitionBy("date").parquet(filled_path(lake))
    return lake


def settle(spark, lake):
    from jobs.dr_settlement import run

    return {r.customer_code: r for r in run(spark, lake, [DAY]).collect()}


def test_baseline_skips_the_customers_own_event_day_and_averages_the_window(spark, tmp_path):
    lake = build_lake(spark, tmp_path, kwh={("M1", date(2013, 7, 12)): 2.0},
                      missing=[("M9", date(2013, 7, d)) for d in (15, 16, 17, 18, 19, 12, 11, 10, 9, 8)])
    rows = settle(spark, lake)
    # CU-1: 19, 18, 16, 15 at 6 kWh and the 12th at 12 kWh (the 17th had an event) -> 7.2
    assert (rows["CU-1"].baseline_days, rows["CU-1"].baseline_kwh) == (5, 7.2)
    # CU-3: two meters, 12 kWh every day
    assert (rows["CU-3"].baseline_days, rows["CU-3"].baseline_kwh) == (5, 12.0)


def test_without_5_days_of_readings_there_is_no_baseline(spark, tmp_path):
    lake = build_lake(spark, tmp_path, missing=[("M9", date(2013, 7, d)) for d in (15, 16, 17)])
    row = settle(spark, lake)["CU-9"]
    assert (row.baseline_days, row.baseline_kwh) == (2, None)          # only the 18th and 19th


def test_rerun_gives_the_same_rows(spark, tmp_path):
    from jobs.dr_settlement import settlement_path

    lake = build_lake(spark, tmp_path)
    settle(spark, lake)
    settle(spark, lake)
    assert spark.read.parquet(settlement_path(lake)).count() == 3


# ---------- T-44.2: saving and incentive ----------

def test_saving_against_the_baseline_and_incentive_from_10_percent(spark, tmp_path):
    lake = build_lake(spark, tmp_path, kwh={("M1", date(2013, 7, 12)): 2.0,           # CU-1 baseline 7.2
                                            ("M3", DAY): 0.92, ("M4", DAY): 0.92})    # CU-3: 11.04 of 12
    rows = settle(spark, lake)
    cu1 = rows["CU-1"]                       # 6 kWh of 7.2 -> 1.2 kWh, 16.67%
    assert (cu1.actual_kwh, cu1.savings_kwh, cu1.savings_pct) == (6.0, 1.2, 0.1667)
    assert (cu1.incentive_paid, cu1.price_per_kwh, cu1.incentive_amount) == (True, 0.30, 0.36)
    cu3 = rows["CU-3"]                       # 8% -> no incentive (CA-07)
    assert (cu3.savings_pct, cu3.incentive_paid, cu3.incentive_amount) == (0.08, False, 0.0)


def test_exactly_10_percent_earns_the_incentive(spark, tmp_path):
    lake = build_lake(spark, tmp_path, kwh={("M3", DAY): 0.9, ("M4", DAY): 0.9})      # 10.8 of 12
    cu3 = settle(spark, lake)["CU-3"]
    assert (cu3.savings_pct, cu3.incentive_paid, cu3.incentive_amount) == (0.1, True, 0.36)


def test_no_baseline_no_incentive(spark, tmp_path):
    lake = build_lake(spark, tmp_path, kwh={("M9", DAY): 0.0},
                      missing=[("M9", date(2013, 7, d)) for d in (15, 16, 17)])
    cu9 = settle(spark, lake)["CU-9"]
    assert (cu9.baseline_kwh, cu9.savings_pct, cu9.incentive_paid, cu9.incentive_amount) == (None, None, False, 0.0)
