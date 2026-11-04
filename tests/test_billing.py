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
