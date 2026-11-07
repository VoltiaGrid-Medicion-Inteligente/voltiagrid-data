"""Demand-response fact per customer and event (US-45, T-45.1)."""

from datetime import date, datetime
from decimal import Decimal

import pytest

from jobs.lake import Lake

DAY = date(2013, 7, 22)
EVENT = "DR-CT-1-20130722"


def build_lake(spark, tmp_path, customers=("CU-1", "CU-9"), f4=("CU-1", "CU-9"), events=(EVENT,)):
    from jobs.dr_events import EVENT_SCHEMA
    from jobs.dr_settlement import settlement_path
    from jobs.inventory import write_raw

    lake = Lake.local(str(tmp_path / "lake"))
    write_raw(lake, {
        "meters": (["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva", "circuit_code",
                    "circuit_capacity_kva"],
                   [(f"M-{c}", c, "TR-1", Decimal("100"), "CT-1", Decimal("1000")) for c in f4]),
        "contracts": (["customer_code", "contract_code", "tariff_type", "start_date", "end_date"], []),
    }, date(2026, 11, 1))
    spark.createDataFrame([(e, "CT-1", datetime(2013, 7, 22, 19), datetime(2013, 7, 22, 22), 3.0, 31.0, 0.9, DAY)
                           for e in events], EVENT_SCHEMA).write.partitionBy("date").parquet(lake.curated("dr_events"))
    measures = {"CU-1": (5, 7.2, 6.0, 1.2, 0.1667, True, 0.3, 0.36), "CU-9": (2, None, 5.0, None, None, False, 0.3, 0.0)}
    spark.createDataFrame([(EVENT, c, *measures.get(c, measures["CU-1"]), DAY) for c in customers],
                          "event_id string, customer_code string, baseline_days int, baseline_kwh double, "
                          "actual_kwh double, savings_kwh double, savings_pct double, incentive_paid boolean, "
                          "price_per_kwh double, incentive_amount double, date date") \
        .write.partitionBy("date").parquet(settlement_path(lake))
    return lake


def test_one_row_per_customer_and_event_with_keys_and_measures(spark, tmp_path):
    from jobs.fact_dr_participation import run

    rows = {r.customer_code: r for r in run(spark, build_lake(spark, tmp_path), [DAY]).collect()}
    assert set(rows) == {"CU-1", "CU-9"}
    r = rows["CU-1"]
    assert (r.date_key, r.event_id, r.circuit_code) == (20130722, EVENT, "CT-1")
    assert (r.baseline_kwh, r.actual_kwh, r.savings_kwh, r.savings_pct, r.incentive_paid, r.incentive_amount) == \
        (7.2, 6.0, 1.2, 0.1667, 1, 0.36)
    assert (rows["CU-9"].baseline_kwh, rows["CU-9"].incentive_paid) == (None, 0)   # took part, not settled


def test_a_customer_missing_from_f4_stops_the_job(spark, tmp_path):
    from jobs.fact_dr_participation import run

    lake = build_lake(spark, tmp_path, customers=("CU-1", "CU-7"), f4=("CU-1",))
    with pytest.raises(SystemExit, match="orphan keys.*CU-7"):
        run(spark, lake, [DAY])


def test_an_event_missing_from_dr_events_stops_the_job(spark, tmp_path):
    from jobs.fact_dr_participation import run

    lake = build_lake(spark, tmp_path, events=("DR-OTHER",))
    with pytest.raises(SystemExit, match="orphan keys.*event_id"):
        run(spark, lake, [DAY])
