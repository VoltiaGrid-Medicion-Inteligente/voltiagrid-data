"""CT-08 (US-39, T-39.3): running the pipeline again for the same date gives the same data, without duplicates.

The DAG can retry a task, an operator can trigger the same date twice, and a resend lands in a partition that was
already processed. Every job overwrites only its own partitions (dynamic overwrite) and recomputes them from its
inputs, so the second run must leave every dataset exactly as the first one did.
"""

import json
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from jobs import billing, circuit_load, estimate_gaps, fact_consumption
from jobs.clean_readings import clean_path, merge_and_write
from jobs.inventory import write_raw
from jobs.lake import Lake
from jobs.readings import read_f3

INGEST = "2026-11-02"
FIRST_DAY = date(2013, 3, 1)
DAYS = 29
METERS = ["MAC000001", "MAC000002", "MAC000003"]


def write_raw_f3(root):
    """One message per day with the 48 readings of 3 meters; day 10 stored twice (consumer crash); a null reading, a
    90-minute gap and 4 missing days."""
    day_folder = root / "raw" / "source=f3" / "dataset=readings" / f"ingest_date={INGEST}" / "ingest_hour=14"
    day_folder.mkdir(parents=True)
    for d in range(DAYS):
        day = FIRST_DAY + timedelta(days=d)
        readings = []
        for m, meter in enumerate(METERS):
            for i in range(48):
                ts = datetime.combine(day, datetime.min.time()) + timedelta(minutes=30 * i)
                kwh = None if (d == 20 and i == 7 and m == 0) else round(0.1 + 0.01 * i + 0.05 * m, 3)
                if d == 28 and m == 1 and 10 <= i < 13:
                    continue  # a 90-minute gap on the last day
                if m == 2 and 3 <= d < 7:
                    continue  # MAC000003 sends nothing for 4 days: 13.8% estimated in March -> inspection order
                readings.append({"meter_id": meter, "interval_start": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                 "energy_kwh": kwh, "reading_seq": d * 1000 + m * 100 + i + 1})
        message = {"schema_version": "1.0", "message_id": f"DC-TR-00001-{d + 1:08d}", "concentrator_id": "DC-TR-00001",
                   "sequence": d + 1, "sent_at": "2013-03-30T00:00:00Z", "is_resend": True, "readings": readings}
        body = json.dumps(message)
        (day_folder / f"DC-TR-00001_{d + 1:08d}.json").write_text(body, encoding="utf-8")
        if d == 10:
            again = day_folder.parent / "ingest_hour=15"
            again.mkdir(exist_ok=True)
            (again / f"DC-TR-00001_{d + 1:08d}.json").write_text(body, encoding="utf-8")


def run_pipeline(spark, lake, raw_root):
    """The DAG's jobs in order, for one ingest date."""
    raw = read_f3(spark, [str(raw_root / "raw" / "source=f3" / "dataset=readings" / f"ingest_date={INGEST}")])
    _, dates = merge_and_write(spark, raw, clean_path(lake))
    days = [d.isoformat() for d in dates]
    estimate_gaps.run(spark, lake, days)
    circuit_load.run(spark, lake, days)
    fact_consumption.run(spark, lake, ["2013-03"])
    billing.run(spark, lake, ["2013-03"])


DATASETS = {
    "clean": lambda lake: clean_path(lake),
    "filled": lambda lake: estimate_gaps.filled_path(lake),
    "circuit_load": lambda lake: lake.curated("circuit_load"),
    "fact_consumption": lambda lake: lake.curated("fact_consumption"),
    "billing_status": lambda lake: lake.curated("billing_status"),
    "inspection_orders": lambda lake: lake.curated("inspection_orders"),
}


def snapshot(spark, lake):
    """Every row of every dataset, without columns that record when the job ran."""
    result = {}
    for name, path in DATASETS.items():
        df = spark.read.parquet(path(lake))
        df = df.drop("created_at") if "created_at" in df.columns else df
        result[name] = sorted(tuple(str(v) for v in row) for row in df.collect())
    return result


@pytest.fixture(scope="module")
def two_runs(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("ct08")
    lake = Lake.local(str(root))
    write_raw(lake, {"meters": (["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva",
                                 "circuit_code", "circuit_capacity_kva"],
                                [(m, f"CU-{m}", "TR-00001", Decimal("100"), "CT-000", Decimal("50")) for m in METERS])},
              date(2026, 11, 1))
    write_raw_f3(root)
    run_pipeline(spark, lake, root)
    first = snapshot(spark, lake)
    run_pipeline(spark, lake, root)
    second = snapshot(spark, lake)
    return lake, first, second


@pytest.mark.parametrize("dataset", list(DATASETS))
def test_ct_08_second_run_leaves_every_dataset_identical(two_runs, dataset):
    _, first, second = two_runs
    assert first[dataset], f"{dataset} is empty: the pipeline did not run"
    assert second[dataset] == first[dataset]


def test_ct_08_no_meter_interval_appears_twice_after_two_runs(spark, two_runs):
    lake, _, _ = two_runs
    for path in (clean_path(lake), estimate_gaps.filled_path(lake)):
        df = spark.read.parquet(path).filter("interval_start is not null")
        assert df.count() == df.select("meter_id", "interval_start").distinct().count()


def test_the_message_stored_twice_did_not_create_duplicates(spark, two_runs):
    lake, _, _ = two_runs
    day_11 = spark.read.parquet(clean_path(lake)).filter("date = '2013-03-11'")
    assert day_11.count() == len(METERS) * 48


def test_one_inspection_order_after_two_runs(spark, two_runs):
    lake, _, _ = two_runs
    orders = spark.read.parquet(lake.curated("inspection_orders")).collect()
    assert [(o.customer_code, o.month) for o in orders] == [("CU-MAC000003", "2013-03")]
