"""Circuit load per interval (US-33, T-33.1)."""

from datetime import datetime
from decimal import Decimal

from jobs.circuit_load import circuit_load

T = datetime(2013, 1, 1, 18, 0)


def meters_frame(spark, rows):
    return spark.createDataFrame(
        [(m, f"CU-{m}", "TR-1", Decimal("100"), c, Decimal(str(cap))) for m, c, cap in rows],
        "meter_id string, customer_code string, transformer_code string, transformer_capacity_kva decimal(8,2), "
        "circuit_code string, circuit_capacity_kva decimal(8,2)")


def readings_frame(spark, rows):
    return spark.createDataFrame(
        [(m, ts, kwh, ok) for m, ts, kwh, ok in rows],
        "meter_id string, interval_start timestamp_ntz, energy_kwh double, is_valid boolean")


def test_load_is_twice_the_half_hour_energy_over_the_capacity(spark):
    meters = meters_frame(spark, [("M1", "CT-1", 10), ("M2", "CT-1", 10)])
    readings = readings_frame(spark, [("M1", T, 2.0, True), ("M2", T, 2.5, True)])
    row = circuit_load(readings, meters).first()
    assert row.energy_kwh == 4.5
    assert row.load_kw == 9.0          # 4.5 kWh in 30 minutes = 9 kW on average
    assert row.load_pct == 0.9
    assert row.is_overloaded is True
    assert (row.meters_reporting, row.meters_total) == (2, 2)
