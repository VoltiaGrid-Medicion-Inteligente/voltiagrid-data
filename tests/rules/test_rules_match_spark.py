"""The Spark jobs give the same answer as the rules in pipeline/ on the CA scenarios (traceability matrix).

test_acceptance.py checks the rules; these tests run the real jobs on the same fixtures, so a rule implemented in
pipeline/ cannot be missing or different in Spark.
"""

from datetime import datetime

from jobs.readings import READING_COLUMNS


def readings_frame(spark, readings):
    """Rule fixtures (meter_id, ts, energy_kwh, seq) as raw readings."""
    rows = [(r["meter_id"], r["ts"], None if r["energy_kwh"] is None else str(r["energy_kwh"]), r["seq"], "f3")
            for r in readings]
    schema = "meter_id string, original_ts timestamp_ntz, energy_text string, seq long, source string"
    return spark.createDataFrame(rows, schema).select(READING_COLUMNS)


# ---------- CA-01, CA-02: jobs.clean_readings ----------

def test_ca_01_spark_normalizes_10_33_and_flags_10_41(spark, make_reading):
    from jobs.clean_readings import clean
    from pipeline.cleaning import normalize_clock

    times = [datetime(2013, 3, 4, 10, 33), datetime(2013, 3, 4, 10, 41)]
    rows = clean(readings_frame(spark, [make_reading(ts=t, seq=i) for i, t in enumerate(times, 1)])).collect()
    by_seq = {r.seq: r for r in rows}
    for seq, t in enumerate(times, 1):
        expected_interval, invalid = normalize_clock(t)
        assert by_seq[seq].interval_start == expected_interval
        assert (by_seq[seq].quality_flag == "invalid_clock") is invalid


def test_ca_02_spark_keeps_the_same_reading_as_the_rule(spark, duplicated_interval):
    from jobs.clean_readings import clean
    from pipeline.cleaning import deduplicate

    expected = deduplicate(duplicated_interval)
    rows = clean(readings_frame(spark, duplicated_interval)).collect()
    assert [(r.seq, r.energy_kwh) for r in rows] == [(e["seq"], e["energy_kwh"]) for e in expected]


# ---------- CA-03: jobs.estimate_gaps ----------

def test_ca_03_spark_estimates_the_same_intervals_with_the_same_method_and_value(spark, series_with_gaps):
    from jobs.clean_readings import clean
    from jobs.estimate_gaps import fill_gaps as spark_fill_gaps
    from pipeline.estimation import fill_gaps

    last_day = max(r["ts"] for r in series_with_gaps).date()
    expected = {r["ts"]: (r["estimation_method"], r["energy_kwh"])
                for r in fill_gaps(series_with_gaps) if r["ts"].date() == last_day}
    rows = spark_fill_gaps(clean(readings_frame(spark, series_with_gaps)), [last_day]).collect()
    assert {r.interval_start: (r.estimation_method, r.energy_kwh) for r in rows} == expected


# ---------- RN-05: jobs.tariff_bands ----------

def test_rn_05_spark_band_equals_the_calendar_lookup_for_every_half_hour(spark):
    from datetime import timedelta

    from jobs.tariff_bands import assign_bands, read_tariffs
    from validate_tariffs import band_for, load_tariffs

    times = [datetime(2011, 12, 31) + i * timedelta(minutes=30) for i in range(96)]  # across the change of period
    readings = spark.createDataFrame([("M1", t) for t in times], "meter_id string, interval_start timestamp_ntz")
    spark_bands = {r.interval_start: (r.band_name, r.price_per_kwh)
                   for r in assign_bands(readings, read_tariffs(spark)).collect()}
    rows = load_tariffs()
    assert spark_bands == {t: (band_for(rows, t).band_name, band_for(rows, t).price_per_kwh) for t in times}


# ---------- CA-04: jobs.billing ----------

def test_ca_04_spark_close_month_takes_the_same_decision_as_the_rule(spark):
    from jobs.billing import close_month as spark_close_month
    from pipeline.billing import close_month

    pcts = [0.12, 0.10, 0.1001, 0.0]
    share = spark.createDataFrame([(f"CU-{i}", "2013-01", p) for i, p in enumerate(pcts)],
                                  "customer_code string, month string, estimated_pct double")
    rows = {r.estimated_pct: (r.inspection_order, r.is_billable) for r in spark_close_month(share).collect()}
    assert rows == {p: (close_month(p)["inspection_order"], close_month(p)["is_billable"]) for p in pcts}


# ---------- CA-05: jobs.transformer_losses and jobs.suspicious_meters ----------

def test_ca_05_spark_flags_the_transformer_on_the_same_day_as_the_rule(spark, transformer_losses):
    from datetime import date, timedelta

    from jobs.transformer_losses import suspicious_transformers
    from pipeline.losses import CONSECUTIVE_DAYS, is_suspicious_transformer

    pcts = transformer_losses["daily_loss_pct"]
    days = [date(2013, 1, 1) + timedelta(days=i) for i in range(len(pcts))]
    losses = spark.createDataFrame(
        [(transformer_losses["transformer"], 100.0, 100.0 * (1 - p), 100.0 * p, p, d) for p, d in zip(pcts, days)],
        "transformer_code string, delivered_kwh double, metered_kwh double, loss_kwh double, loss_pct double, date date")
    spark_days = {r.date for r in suspicious_transformers(losses).collect()}
    # the rule on the 3 days that end on each day
    n = CONSECUTIVE_DAYS
    rule_days = {days[i] for i in range(n - 1, len(pcts)) if is_suspicious_transformer(pcts[i - n + 1: i + 1])}
    assert spark_days == rule_days == {days[3]}


def test_ca_05_spark_meter_decision_equals_the_rule(spark):
    from datetime import datetime, timedelta

    from jobs.suspicious_meters import evaluate_meters
    from pipeline.losses import is_suspicious_meter

    # meter: (zero readings in a row, drop_pct, contract changed)
    cases = {"zero96": (96, None, False), "zero95": (95, None, False), "drop61": (0, 0.61, False),
             "drop60": (0, 0.60, False), "drop_contract": (0, 0.9, True), "both": (100, 0.7, False)}
    candidates = spark.createDataFrame([(m, "TR-1", f"CU-{m}") for m in cases],
                                       "meter_id string, transformer_code string, customer_code string")
    start = datetime(2013, 1, 15)
    zeros = [(m, start + i * timedelta(minutes=30), 0.0, False) for m, (n, _, _) in cases.items() for i in range(n)]
    zero_window = spark.createDataFrame(zeros, "meter_id string, interval_start timestamp_ntz, energy_kwh double, "
                                               "is_estimated boolean")
    drops = spark.createDataFrame([(m, c[1]) for m, c in cases.items()], "meter_id string, drop_pct double")
    changes = spark.createDataFrame([(f"CU-{m}", True) for m, c in cases.items() if c[2]],
                                    "customer_code string, contract_changed boolean")
    decided = {r.meter_id: r.is_suspicious for r in evaluate_meters(candidates, zero_window, drops, changes).collect()}
    assert decided == {m: is_suspicious_meter(*c) for m, c in cases.items()}
