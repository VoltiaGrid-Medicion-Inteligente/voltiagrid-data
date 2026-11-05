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
