"""Billing check (US-36): estimated share per customer and month (T-36.1)."""

from decimal import Decimal

from jobs.billing import estimated_share


def fact_frame(spark, rows):
    """rows: (customer, month, band, intervals, estimated_intervals)"""
    return spark.createDataFrame(
        [(c, b, None, Decimal("0.3"), 1.0, 0.0, n, e, Decimal("0.30"), m) for c, m, b, n, e in rows],
        "customer_code string, band_name string, tariff_valid_from date, price_per_kwh decimal(10,4), "
        "energy_kwh double, estimated_kwh double, intervals long, estimated_intervals long, amount decimal(12,2), "
        "month string")


def test_estimated_share_adds_every_band_of_the_month(spark):
    fact = fact_frame(spark, [
        ("CU-1", "2013-01", "peak", 248, 30), ("CU-1", "2013-01", "off-peak", 992, 120),
        ("CU-1", "2013-01", "intermediate", 1248, 148),                                   # 298 of 2,488 = 11.98%
        ("CU-2", "2013-01", "peak", 248, 0), ("CU-1", "2013-02", "peak", 224, 1),
    ])
    rows = {(r.customer_code, r.month): r for r in estimated_share(fact).collect()}
    cu1 = rows[("CU-1", "2013-01")]
    assert (cu1.intervals, cu1.estimated_intervals, cu1.estimated_pct) == (2488, 298, 0.1198)
    assert rows[("CU-2", "2013-01")].estimated_pct == 0.0
    assert len(rows) == 3


# ---------- T-36.2: billable or inspection order ----------

def share_frame(spark, pcts):
    return spark.createDataFrame([(f"CU-{i}", "2013-01", 1000, int(p * 1000), p) for i, p in enumerate(pcts)],
                                 "customer_code string, month string, intervals long, estimated_intervals long, "
                                 "estimated_pct double")


def test_more_than_10_percent_gets_an_inspection_order_and_no_bill(spark):
    from jobs.billing import close_month

    rows = {r.estimated_pct: r for r in close_month(share_frame(spark, [0.12, 0.10, 0.1001, 0.0])).collect()}
    assert (rows[0.12].inspection_order, rows[0.12].is_billable) == (True, False)       # CA-04
    assert (rows[0.10].inspection_order, rows[0.10].is_billable) == (False, True)       # exactly 10% is billable
    assert rows[0.1001].inspection_order is True
    assert rows[0.0].is_billable is True


def test_the_order_records_the_reason_and_the_percentage(spark):
    from datetime import datetime

    from jobs.billing import REASON, close_month, inspection_orders

    orders = inspection_orders(close_month(share_frame(spark, [0.12, 0.05])), datetime(2013, 2, 1, 6, 0)).collect()
    assert len(orders) == 1
    order = orders[0]
    assert order.order_id == "IO-2013-01-CU-0"
    assert (order.estimated_pct, order.estimated_intervals, order.intervals) == (0.12, 120, 1000)
    assert order.reason == REASON and "10%" in order.reason
    assert order.created_at == datetime(2013, 2, 1, 6, 0)


def test_closing_the_month_twice_keeps_one_order_per_customer(spark, tmp_path):
    from jobs.billing import run
    from jobs.lake import Lake

    lake = Lake.local(str(tmp_path / "lake"))
    fact_frame(spark, [("CU-1", "2013-01", "peak", 100, 12), ("CU-2", "2013-01", "peak", 100, 1)]) \
        .write.partitionBy("month").parquet(lake.curated("fact_consumption"))
    run(spark, lake, ["2013-01"])
    run(spark, lake, ["2013-01"])
    orders = spark.read.parquet(lake.curated("inspection_orders")).collect()
    assert [o.customer_code for o in orders] == ["CU-1"]
    status = {r.customer_code: r.is_billable for r in spark.read.parquet(lake.curated("billing_status")).collect()}
    assert status == {"CU-1": False, "CU-2": True}
