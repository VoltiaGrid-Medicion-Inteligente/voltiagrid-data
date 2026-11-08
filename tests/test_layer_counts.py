"""Control counts between layers of a cleaning run (US-41, T-41.3)."""

import json
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from jobs.lake import Lake

INGEST = "2026-11-02"
FIRST_DAY = date(2013, 3, 1)
DAYS = 8
METERS = ["MAC000001", "MAC000002", "MAC000003"]


def write_raw_f3(root):
    """One message per day with 48 readings of 3 meters; a null reading and a 90-minute gap; day 3 stored twice."""
    folder = root / "raw" / "source=f3" / "dataset=readings" / f"ingest_date={INGEST}" / "ingest_hour=14"
    folder.mkdir(parents=True)
    for d in range(DAYS):
        day = FIRST_DAY + timedelta(days=d)
        readings = []
        for m, meter in enumerate(METERS):
            for i in range(48):
                if d == 7 and m == 1 and 10 <= i < 13:
                    continue
                ts = datetime.combine(day, datetime.min.time()) + timedelta(minutes=30 * i)
                kwh = None if (d == 5 and i == 7 and m == 0) else round(0.1 + 0.01 * i + 0.05 * m, 3)
                readings.append({"meter_id": meter, "interval_start": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                 "energy_kwh": kwh, "reading_seq": d * 1000 + m * 100 + i + 1})
        body = json.dumps({"schema_version": "1.0", "message_id": f"DC-1-{d}", "concentrator_id": "DC-1",
                           "sequence": d + 1, "sent_at": "2013-03-30T00:00:00Z", "is_resend": False,
                           "readings": readings})
        (folder / f"DC-1_{d + 1:08d}.json").write_text(body, encoding="utf-8")
        if d == 3:
            (folder / f"DC-1_{d + 1:08d}_again.json").write_text(body, encoding="utf-8")


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory):
    """The pipeline of the run f3-2026-11-02, as the DAG runs it; MAC000003 is not in F4."""
    from jobs import estimate_gaps, fact_consumption
    from jobs.clean_readings import clean_path, merge_and_write
    from jobs.inventory import write_raw
    from jobs.lake import write_text
    from jobs.readings import read_f3

    root = tmp_path_factory.mktemp("layers")
    lake = Lake.local(str(root))
    write_raw(lake, {"meters": (["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva",
                                 "circuit_code", "circuit_capacity_kva"],
                                [(m, f"CU-{m}", "TR-1", Decimal("100"), "CT-1", Decimal("50")) for m in METERS[:2]])},
              date(2026, 11, 1))
    write_raw_f3(root)
    raw_folder = str(root / "raw" / "source=f3" / "dataset=readings" / f"ingest_date={INGEST}")
    _, dates = merge_and_write(spark, read_f3(spark, [raw_folder]), clean_path(lake))
    days = [d.isoformat() for d in dates]
    write_text(spark, f"{clean_path(lake)}/_runs/f3-{INGEST}.json",
               json.dumps({"run": f"f3-{INGEST}", "source": "f3", "input": raw_folder, "dates": days}))
    estimate_gaps.run(spark, lake, days)
    fact_consumption.run(spark, lake, ["2013-03"])
    return lake


def test_every_layer_agrees_for_the_run(spark, lake):
    from jobs.layer_counts import failures, run

    counts = run(spark, lake, f"f3-{INGEST}")
    assert counts["raw_readings"] == 3 * 48 * DAYS - 3 + 3 * 48             # the gap; day 4 twice
    assert counts["raw_meter_intervals"] == 3 * 48 * DAYS - 3
    assert (counts["lost_raw_to_clean"], counts["duplicated_in_clean"]) == (0, 0)
    assert counts["filled_real"] == counts["clean_valid"] == 3 * 48 * DAYS - 3 - 1   # the null is not valid
    assert counts["filled_estimated"] == 3 + 1                                # the gap and the null, estimated
    assert counts["fact_intervals"] == counts["filled_month_rows_in_f4"] == 2 * 48 * DAYS
    assert counts["meters_without_f4"] == 1                                  # MAC000003: reported, not billed
    assert failures(counts) == []


def test_a_difference_between_filled_and_curated_fails_the_check():
    from jobs.layer_counts import failures

    ok = {"lost_raw_to_clean": 0, "duplicated_in_clean": 0, "filled_real": 10, "clean_valid": 10,
          "fact_intervals": 96, "filled_month_rows_in_f4": 96, "fact_kwh": 100.0, "filled_month_kwh_in_f4": 100.0}
    assert failures(ok) == []
    assert failures(ok | {"fact_intervals": 95}) == ["filled -> curated: intervals differ"]
    assert failures(ok | {"fact_kwh": 100.5}) == ["filled -> curated: kWh differ"]
    assert failures(ok | {"fact_kwh": 100.005}) == []                        # rounding of the monthly fact
    assert failures(ok | {"filled_real": 9}) == ["clean -> filled: valid readings missing or added"]
    assert failures(ok | {"lost_raw_to_clean": 1}) == ["raw -> clean lost or duplicated readings"]
