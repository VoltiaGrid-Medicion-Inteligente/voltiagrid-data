"""Quality checks of the lake after a run (US-49, T-49.3)."""

from datetime import date, datetime, timedelta

D = date(2013, 3, 1)


def frames(spark, filled_rows, clean_bad=False, billing_bad=False):
    clean = spark.createDataFrame(
        [("M1", datetime(2013, 3, 1, 0, 0), -1.0 if clean_bad else 0.5, True, "ok", D)],
        "meter_id string, interval_start timestamp_ntz, energy_kwh double, is_valid boolean, quality_flag string, "
        "date date")
    filled = spark.createDataFrame(filled_rows, "meter_id string, interval_start timestamp_ntz, energy_kwh double, "
                                                "is_estimated boolean, estimation_method string, date date")
    circuit = spark.createDataFrame([("CT-1", 0.5, 100.0, D)], "circuit_code string, load_pct double, "
                                                               "capacity_kva double, date date")
    fact = spark.createDataFrame([("CU-1", "peak", 0.3, 10.0, 1.0, 48, 2, 3.0, "2013-03")],
                                 "customer_code string, band_name string, price_per_kwh double, energy_kwh double, "
                                 "estimated_kwh double, intervals long, estimated_intervals long, amount double, "
                                 "month string")
    billing = spark.createDataFrame([("CU-1", 0.04, True, billing_bad, "2013-03")],
                                    "customer_code string, estimated_pct double, is_billable boolean, "
                                    "inspection_order boolean, month string")
    return {"clean": clean, "filled": filled, "circuit_load": circuit, "fact_consumption": fact,
            "billing_status": billing}


def day(meter="M1", n=48, energy=0.5, method=None):
    start = datetime(2013, 3, 1)
    return [(meter, start + timedelta(minutes=30 * i), energy, method is not None, method, D) for i in range(n)]


def bad(results):
    return {(name, check) for name, check, count in results if count}


def test_a_good_run_passes_every_check(spark):
    from jobs.quality_checks import run_checks

    assert bad(run_checks(frames(spark, day()))) == set()


def test_negative_energy_an_incomplete_day_and_a_contradiction_are_caught(spark):
    from jobs.quality_checks import run_checks

    rows = day("M1") + day("M2", n=47) + [("M3", datetime(2013, 3, 1), -0.2, False, None, D)] + day("M3", n=47)[1:] + \
        [("M3", datetime(2013, 3, 1, 23, 30), 0.5, False, None, D)]
    found = bad(run_checks(frames(spark, rows, clean_bad=True, billing_bad=True)))
    assert ("filled", "48 intervals per meter and day") in found                 # M2 has 47
    assert ("filled", "energy in 0..25 kWh") in found                            # M3 has -0.2
    assert ("clean", "valid energy in 0..25 kWh") in found
    assert ("billing_status", "billable or inspection, never both") in found


def test_missing_energy_is_only_allowed_when_it_could_not_be_estimated(spark):
    from jobs.quality_checks import run_checks

    unavailable = day(energy=None, method="unavailable")
    assert bad(run_checks(frames(spark, unavailable))) == set()
    silent = day(energy=None, method=None)
    assert ("filled", "no energy only when unavailable") in bad(run_checks(frames(spark, silent)))
