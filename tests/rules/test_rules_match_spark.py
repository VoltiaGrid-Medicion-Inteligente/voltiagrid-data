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
