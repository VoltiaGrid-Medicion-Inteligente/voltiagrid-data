"""Loss fact per transformer and day (US-51): measures and keys (T-51.1), value in money (T-51.2)."""

import json
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from jobs.lake import Lake

DAY = date(2013, 1, 20)
HALF_HOUR = timedelta(minutes=30)


def write_day_inputs(spark, lake, transformers, delivered_per_quarter=1.0, metered_per_half_hour=1.7):
    """F5 raw (96 quarters) and filled readings (one meter M-<transformer>, 48 half hours) of DAY."""
    from jobs.estimate_gaps import filled_path
    from jobs.fetch_f5 import object_name
    from jobs.lake import put_object

    start = datetime.combine(DAY, datetime.min.time())
    rows = []
    for t in transformers:
        answer = {"transformer_code": t, "interval_minutes": 15,
                  "readings": [{"interval_start": (start + i * timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "delivered_kwh": delivered_per_quarter} for i in range(96)]}
        put_object(f"{lake.raw('f5', 'transformer_energy')}/ingest_date=2026-11-01/{object_name(t, DAY)}",
                   json.dumps(answer))
        rows += [(f"M-{t}", start + i * HALF_HOUR, metered_per_half_hour, False, None, "ok", DAY) for i in range(48)]
    spark.createDataFrame(rows, "meter_id string, interval_start timestamp_ntz, energy_kwh double, is_estimated boolean, "
                                "estimation_method string, quality_flag string, date date") \
        .write.mode("overwrite").partitionBy("date").parquet(filled_path(lake))


def build_lake(spark, tmp_path, daily_pct, f4=("TR-1", "TR-2")):
    """daily_pct: {transformer: [loss_pct of DAY-n+1 .. DAY]} (None = no row that day); 100 kWh delivered a day."""
    from jobs.inventory import write_raw
    from jobs.transformer_losses import losses_path, suspicious_path, suspicious_transformers

    lake = Lake.local(str(tmp_path / "lake"))
    write_raw(lake, {
        "meters": (["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva", "circuit_code",
                    "circuit_capacity_kva"],
                   [(f"M-{t}", f"CU-{t}", t, Decimal("100"), f"CT-{t[-1]}", Decimal("1000")) for t in f4]),
        "contracts": (["customer_code", "contract_code", "tariff_type", "start_date", "end_date"], []),
    }, date(2026, 11, 1))
    rows = []
    for t, pcts in daily_pct.items():
        for i, pct in enumerate(pcts):
            if pct is None:
                continue
            d = DAY - timedelta(days=len(pcts) - 1 - i)
            rows.append((t, 100.0, 100.0 * (1 - pct), 100.0 * pct, pct, 0.0, 48, 5, d))
    losses = spark.createDataFrame(rows, "transformer_code string, delivered_kwh double, metered_kwh double, "
                                         "loss_kwh double, loss_pct double, estimated_kwh double, intervals long, "
                                         "meters long, date date")
    losses.write.partitionBy("date").parquet(losses_path(lake))
    suspicious_transformers(losses).write.partitionBy("date").parquet(suspicious_path(lake))
    write_day_inputs(spark, lake, list(daily_pct))
    return lake


def fact(spark, lake):
    from jobs.fact_transformer_loss import run

    return {r.transformer_code: r for r in run(spark, lake, [DAY]).collect()}


def test_one_row_per_transformer_and_day_with_circuit_and_keys(spark, tmp_path):
    rows = fact(spark, build_lake(spark, tmp_path, {"TR-1": [0.05], "TR-2": [0.2]}))
    assert set(rows) == {"TR-1", "TR-2"}
    r = rows["TR-2"]
    assert (r.date_key, r.circuit_code, r.delivered_kwh, r.loss_kwh, r.loss_pct) == (20130120, "CT-2", 100.0, 20.0, 0.2)


def test_consecutive_days_and_suspicious_flag_follow_rn_06(spark, tmp_path):
    rows = fact(spark, build_lake(spark, tmp_path, {"TR-1": [0.15, 0.15, 0.15, 0.15], "TR-2": [0.15, None, 0.15, 0.15]}))
    assert (rows["TR-1"].consecutive_days_over_12, rows["TR-1"].is_suspicious, rows["TR-1"].suspicious_rank) == (4, 1, 1)
    assert (rows["TR-2"].consecutive_days_over_12, rows["TR-2"].is_suspicious, rows["TR-2"].suspicious_rank) == \
        (2, 0, None)                                       # the missing day broke the streak


def test_a_day_at_12_percent_has_no_streak(spark, tmp_path):
    rows = fact(spark, build_lake(spark, tmp_path, {"TR-1": [0.15, 0.15, 0.12]}))
    assert (rows["TR-1"].consecutive_days_over_12, rows["TR-1"].is_suspicious) == (0, 0)


def test_a_transformer_missing_from_f4_stops_the_job(spark, tmp_path):
    lake = build_lake(spark, tmp_path, {"TR-1": [0.05], "TR-9": [0.05]}, f4=("TR-1",))
    with pytest.raises(SystemExit, match="orphan keys.*TR-9"):
        fact(spark, lake)


# ---------- T-51.2: loss valued with F7 ----------

def test_each_half_hour_loss_is_valued_at_the_price_of_its_band(spark, tmp_path):
    from jobs.fact_transformer_loss import run as fact_run
    from jobs.inventory import write_raw
    from jobs.transformer_losses import run as losses_run

    lake = Lake.local(str(tmp_path / "lake"))
    write_raw(lake, {
        "meters": (["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva", "circuit_code",
                    "circuit_capacity_kva"], [("M-TR-1", "CU-1", "TR-1", Decimal("100"), "CT-1", Decimal("1000"))]),
        "contracts": (["customer_code", "contract_code", "tariff_type", "start_date", "end_date"], []),
    }, date(2026, 11, 1))
    write_day_inputs(spark, lake, ["TR-1"])                 # 2.0 delivered, 1.7 metered: 0.3 kWh lost each half hour
    losses_run(spark, lake, [DAY])                          # RN-06 first, as in the DAG
    row = fact_run(spark, lake, [DAY]).first()
    assert row.loss_kwh == 14.4                             # 48 x 0.3
    # 2013 prices: 20 off-peak x 0.12 + 20 intermediate x 0.18 + 8 peak x 0.30 = 8.4 per kWh-per-half-hour
    assert row.loss_cost == Decimal("2.52")                 # 0.3 x 8.4, not 14.4 x one average price
