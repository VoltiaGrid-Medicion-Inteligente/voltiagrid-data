"""CA-08 on the real jobs (US-31, T-31.1): a 24-hour resend of 1,000 meters, as the F3 consumer stores it in raw
(some messages twice, after a crash), goes through the cleaning job without losing or duplicating a reading."""

import json
import random
from datetime import datetime, timedelta

import pytest

from jobs.clean_readings import clean_path, merge_and_write
from jobs.control_counts import control_counts
from jobs.lake import Lake
from jobs.readings import read_f3

METERS, HOURS = 1000, 24
METERS_PER_CONCENTRATOR = 64
START = datetime(2013, 1, 1)


def write_resend(raw_day, redelivered=0.05, seed=7):
    """16 concentrators x 48 messages; 5% of the messages stored a second time in another ingest hour."""
    rng = random.Random(seed)
    meters = [f"MAC{m:06d}" for m in range(METERS)]
    groups = [meters[i:i + METERS_PER_CONCENTRATOR] for i in range(0, METERS, METERS_PER_CONCENTRATOR)]
    files = 0
    for c, group in enumerate(groups):
        cid = f"DC-TR-{c:05d}"
        for seq in range(1, HOURS * 2 + 1):
            ts = START + (seq - 1) * timedelta(minutes=30)
            message = {"schema_version": "1.0", "message_id": f"{cid}-{seq:08d}", "concentrator_id": cid,
                       "sequence": seq, "sent_at": "2013-01-02T00:30:00Z", "is_resend": True,
                       "readings": [{"meter_id": m, "interval_start": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                     "energy_kwh": round(rng.uniform(0.05, 1.5), 3), "reading_seq": seq * 100 + j}
                                    for j, m in enumerate(group)]}
            hours = ["14"] + (["15"] if rng.random() < redelivered else [])
            for hour in hours:
                folder = raw_day / f"ingest_hour={hour}"
                folder.mkdir(parents=True, exist_ok=True)
                (folder / f"{cid}_{seq:08d}.json").write_text(json.dumps(message), encoding="utf-8")
                files += 1
    return files


@pytest.fixture(scope="module")
def resend_lake(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("ca08")
    lake = Lake.local(str(root / "lake"))
    raw_day = root / "lake" / "raw" / "source=f3" / "dataset=readings" / "ingest_date=2026-11-02"
    files = write_resend(raw_day)
    raw = read_f3(spark, [str(raw_day)])
    merge_and_write(spark, raw, clean_path(lake))
    return lake, raw, files


def test_ca_08_resend_of_1000_meters_for_24_hours_keeps_every_reading_once(spark, resend_lake):
    lake, raw, files = resend_lake
    assert files > 16 * 48                                   # some messages were stored twice
    counts = control_counts(raw, spark.read.parquet(clean_path(lake)))
    assert counts["raw_readings"] > METERS * HOURS * 2       # the duplicates did arrive...
    assert counts["raw_meter_intervals"] == METERS * HOURS * 2
    assert counts["clean_rows"] == METERS * HOURS * 2 == 48_000
    assert counts["lost"] == 0 and counts["duplicated"] == 0  # ...and none reached clean


def test_ca_08_processing_the_resend_again_changes_nothing(spark, resend_lake):
    lake, raw, _ = resend_lake
    merge_and_write(spark, raw, clean_path(lake))
    counts = control_counts(raw, spark.read.parquet(clean_path(lake)))
    assert (counts["clean_rows"], counts["lost"], counts["duplicated"]) == (48_000, 0, 0)


def test_a_reading_missing_from_clean_is_reported_as_lost(spark, resend_lake):
    from pyspark.sql import functions as F

    lake, raw, _ = resend_lake
    clean_rows = spark.read.parquet(clean_path(lake)).filter(~((F.col("meter_id") == "MAC000001")
                                                               & (F.col("interval_start") == START)))
    assert control_counts(raw, clean_rows)["lost"] == 1
