"""The optimized jobs keep their plans (US-42, T-42.2): the F4 inventory is always broadcast, never shuffled."""

from datetime import datetime
from decimal import Decimal


def plan(df) -> str:
    return df._jdf.queryExecution().executedPlan().toString()


def meters(spark):
    return spark.createDataFrame(
        [("M1", "CU-1", "TR-1", Decimal("100"), "CT-1", Decimal("1000"))],
        "meter_id string, customer_code string, transformer_code string, transformer_capacity_kva decimal(8,2), "
        "circuit_code string, circuit_capacity_kva decimal(8,2)")


def test_circuit_load_broadcasts_f4(spark):
    from jobs.circuit_load import circuit_load

    readings = spark.createDataFrame([("M1", datetime(2013, 1, 1, 18), 1.0, True)],
                                     "meter_id string, interval_start timestamp_ntz, energy_kwh double, is_valid boolean")
    assert "BroadcastHashJoin" in plan(circuit_load(readings, meters(spark)))


def test_monthly_consumption_broadcasts_f4_and_the_tariffs(spark):
    from jobs.fact_consumption import monthly_consumption
    from jobs.tariff_bands import read_tariffs

    filled = spark.createDataFrame([("M1", datetime(2013, 1, 1, 18), 1.0, False)],
                                   "meter_id string, interval_start timestamp_ntz, energy_kwh double, is_estimated boolean")
    text = plan(monthly_consumption(filled, meters(spark), read_tariffs(spark)))
    assert "BroadcastHashJoin" in text and "SortMergeJoin" not in text


def test_transformer_meters_broadcast_f4(spark):
    from jobs.transformer_losses import metered_by_transformer

    filled = spark.createDataFrame([("M1", datetime(2013, 1, 1, 18), 1.0, False)],
                                   "meter_id string, interval_start timestamp_ntz, energy_kwh double, is_estimated boolean")
    assert "BroadcastHashJoin" in plan(metered_by_transformer(filled, meters(spark)))
