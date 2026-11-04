"""Transformer losses (US-37): F5 to raw and 15 -> 30 minute alignment (T-37.1)."""

import json
from datetime import date, datetime, timedelta

from jobs.fetch_f5 import fetch_day, object_name
from jobs.lake import Lake
from jobs.transformer_losses import f5_paths, read_f5, to_half_hour

DAY = date(2013, 1, 5)


def f5_answer(code, day, kwh=1.0, quarters=96):
    start = datetime.combine(day, datetime.min.time())
    return {"transformer_code": code, "interval_minutes": 15,
            "readings": [{"interval_start": (start + i * timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                          "delivered_kwh": kwh} for i in range(quarters)]}


class FakeF5:
    def __init__(self, codes):
        self.codes, self.urls = codes, []

    def __call__(self, url):
        self.urls.append(url)
        if url.endswith("/transformers"):
            return json.dumps([{"transformer_code": c, "meters": 50} for c in self.codes]).encode()
        code = url.split("/transformers/")[1].split("/")[0]
        return json.dumps(f5_answer(code, DAY)).encode()


def test_fetch_stores_one_object_per_transformer_and_day_as_received(tmp_path):
    lake = Lake.local(str(tmp_path / "lake"))
    f5 = FakeF5(["TR-00001", "TR-00002"])
    written = fetch_day(lake, "http://f5", DAY, date(2026, 11, 2), get=f5)
    assert [p.rsplit("/", 1)[1] for p in written] == ["TR-00001_20130105T0000Z_20130106T0000Z.json",
                                                      "TR-00002_20130105T0000Z_20130106T0000Z.json"]
    assert "ingest_date=2026-11-02" in written[0]
    assert "start=2013-01-05T00%3A00%3A00Z&end=2013-01-06T00%3A00%3A00Z" in f5.urls[1]
    assert json.loads(open(written[0]).read())["interval_minutes"] == 15


def test_object_name_has_no_characters_windows_rejects():
    assert ":" not in object_name("TR-00001", DAY)


def test_two_quarters_make_one_half_hour(spark, tmp_path):
    lake = Lake.local(str(tmp_path / "lake"))
    fetch_day(lake, "http://f5", DAY, date(2026, 11, 2), get=FakeF5(["TR-00001"]))
    half_hours = to_half_hour(read_f5(spark, f5_paths(lake, [DAY]))).collect()
    assert len(half_hours) == 48
    assert {r.delivered_kwh for r in half_hours} == {2.0}
    assert min(r.interval_start for r in half_hours) == datetime(2013, 1, 5, 0, 0)


def test_a_day_fetched_twice_counts_once(spark, tmp_path):
    lake = Lake.local(str(tmp_path / "lake"))
    for ingest in (date(2026, 11, 2), date(2026, 11, 3)):
        fetch_day(lake, "http://f5", DAY, ingest, get=FakeF5(["TR-00001"]))
    assert read_f5(spark, f5_paths(lake, [DAY])).count() == 96


def test_a_half_hour_with_one_quarter_is_dropped(spark):
    f5 = spark.createDataFrame([("TR-1", datetime(2013, 1, 5, 0, 0), 1.0), ("TR-1", datetime(2013, 1, 5, 0, 15), 1.5),
                                ("TR-1", datetime(2013, 1, 5, 0, 30), 1.0)],
                               "transformer_code string, interval_start timestamp_ntz, delivered_kwh double")
    rows = to_half_hour(f5).collect()
    assert [(r.interval_start, r.delivered_kwh) for r in rows] == [(datetime(2013, 1, 5, 0, 0), 2.5)]


# ---------- T-37.2: daily loss per transformer ----------

def delivered_frame(spark, rows):
    return spark.createDataFrame(rows, "transformer_code string, interval_start timestamp_ntz, delivered_kwh double")


def metered_frame(spark, rows):
    return spark.createDataFrame(rows, "transformer_code string, interval_start timestamp_ntz, metered_kwh double, "
                                       "estimated_kwh double, meters long")


def test_loss_is_delivered_minus_meters_over_delivered(spark):
    from jobs.transformer_losses import daily_losses

    t = datetime(2013, 1, 5, 10, 0)
    delivered = delivered_frame(spark, [("TR-1", t, 11.5), ("TR-1", t + timedelta(minutes=30), 11.5)])
    metered = metered_frame(spark, [("TR-1", t, 10.0, 0.0, 50), ("TR-1", t + timedelta(minutes=30), 10.0, 1.0, 50)])
    row = daily_losses(delivered, metered).first()
    assert (row.delivered_kwh, row.metered_kwh, row.loss_kwh) == (23.0, 20.0, 3.0)
    assert row.loss_pct == 0.1304          # a 15% injected loss is 13% of what was delivered
    assert (row.intervals, row.meters, row.estimated_kwh) == (2, 50, 1.0)


def test_only_half_hours_present_on_both_sides_are_compared(spark):
    from jobs.transformer_losses import daily_losses

    t = datetime(2013, 1, 5, 10, 0)
    delivered = delivered_frame(spark, [("TR-1", t, 10.0), ("TR-1", t + timedelta(minutes=30), 99.0)])
    metered = metered_frame(spark, [("TR-1", t, 10.0, 0.0, 1)])
    row = daily_losses(delivered, metered).first()
    assert (row.loss_kwh, row.intervals) == (0.0, 1)


def test_meters_are_added_per_transformer_with_their_estimates(spark):
    from decimal import Decimal

    from jobs.transformer_losses import metered_by_transformer

    t = datetime(2013, 1, 5, 10, 0)
    filled = spark.createDataFrame([("M1", t, 1.0, False), ("M2", t, 0.5, True), ("M3", t, 2.0, False)],
                                   "meter_id string, interval_start timestamp_ntz, energy_kwh double, is_estimated boolean")
    meters = spark.createDataFrame(
        [(m, f"CU-{m}", tx, Decimal("100"), "CT-1", Decimal("1000")) for m, tx in (("M1", "TR-1"), ("M2", "TR-1"), ("M3", "TR-2"))],
        "meter_id string, customer_code string, transformer_code string, transformer_capacity_kva decimal(8,2), "
        "circuit_code string, circuit_capacity_kva decimal(8,2)")
    rows = {r.transformer_code: r for r in metered_by_transformer(filled, meters).collect()}
    assert (rows["TR-1"].metered_kwh, rows["TR-1"].estimated_kwh, rows["TR-1"].meters) == (1.5, 0.5, 2)
    assert rows["TR-2"].metered_kwh == 2.0
