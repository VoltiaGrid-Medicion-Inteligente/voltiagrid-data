"""The daily pipeline from its command lines, as Airflow runs it, on a small lake (US-49, T-49.1).

Unit tests call the jobs' functions; this test starts every job with `python -m jobs.<job> ...` in a new process, in
the DAG's order, so it also catches what only the command line has: arguments, the run manifest, reading the lake
written by the previous job, exit codes. Marked `smoke`: the CI runs it in its own job (pipeline-smoke) and the
`tests` job skips it. Locally: python -m pytest -m smoke -v
"""

import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke

INGEST = "2026-11-02"
RUN = f"f3-{INGEST}"
FIRST_DAY = date(2013, 3, 1)
DAYS = 29
METERS = ["MAC000001", "MAC000002", "MAC000003"]
REPO = Path(__file__).resolve().parents[1]


def build_lake(root: Path) -> None:
    """F3 (3 meters, 29 days, a null and a 90-minute gap), F4 meters and contracts, and F5 of the same days."""
    from jobs.fetch_f5 import object_name
    from jobs.inventory import write_raw
    from jobs.lake import Lake, put_object

    lake = Lake.local(str(root))
    write_raw(lake, {
        "meters": (["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva", "circuit_code",
                    "circuit_capacity_kva"],
                   [(m, f"CU-{m}", "TR-00001", Decimal("100"), "CT-000", Decimal("50")) for m in METERS]),
        "contracts": (["customer_code", "contract_code", "tariff_type", "start_date", "end_date"],
                      [(f"CU-{m}", f"CT-{m}", "standard", date(2011, 11, 1), None) for m in METERS]),
    }, date(2026, 11, 1))
    folder = root / "raw" / "source=f3" / "dataset=readings" / f"ingest_date={INGEST}" / "ingest_hour=14"
    folder.mkdir(parents=True)
    for d in range(DAYS):
        day = FIRST_DAY + timedelta(days=d)
        start = datetime.combine(day, datetime.min.time())
        readings = []
        for m, meter in enumerate(METERS):
            for i in range(48):
                if d == 28 and m == 1 and 10 <= i < 13:
                    continue
                kwh = None if (d == 20 and i == 7 and m == 0) else round(0.1 + 0.01 * i + 0.05 * m, 3)
                readings.append({"meter_id": meter, "interval_start": (start + timedelta(minutes=30 * i))
                                 .strftime("%Y-%m-%dT%H:%M:%SZ"), "energy_kwh": kwh, "reading_seq": i + 1})
        (folder / f"DC-TR-00001_{d + 1:08d}.json").write_text(json.dumps(
            {"schema_version": "1.0", "message_id": f"DC-{d}", "concentrator_id": "DC-TR-00001", "sequence": d + 1,
             "sent_at": "2013-03-30T00:00:00Z", "is_resend": False, "readings": readings}), encoding="utf-8")
        f5 = {"transformer_code": "TR-00001", "interval_minutes": 15,
              "readings": [{"interval_start": (start + timedelta(minutes=15 * q)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "delivered_kwh": 0.3} for q in range(96)]}
        put_object(f"{lake.raw('f5', 'transformer_energy')}/ingest_date={INGEST}/{object_name('TR-00001', day)}",
                   json.dumps(f5))


def job(*args: str, lake: Path) -> str:
    """Run one job like the DAG does; fail the test with its output if it exits with an error."""
    command = [sys.executable, "-m", *args, "--lake", str(lake)]
    done = subprocess.run(command, cwd=REPO, capture_output=True, text=True, timeout=600)
    assert done.returncode == 0, f"{' '.join(args)} exited {done.returncode}\n{done.stdout}\n{done.stderr[-3000:]}"
    return done.stdout


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    root = tmp_path_factory.mktemp("smoke")
    build_lake(root)
    return root


def test_the_daily_pipeline_runs_from_its_command_lines(lake):
    out = job("jobs.clean_readings", "--source", "f3", "--ingest-date", INGEST, lake=lake)
    assert f"clean: {3 * 48 * DAYS - 3:,} rows in {DAYS} dates" in out
    assert "estimate_gaps: " in job("jobs.estimate_gaps", "--run", RUN, lake=lake)
    assert "OK: nothing lost, nothing duplicated" in job("jobs.control_counts", "--ingest-date", INGEST, lake=lake)
    assert "circuit_load: " in job("jobs.circuit_load", "--run", RUN, lake=lake)
    job("jobs.fact_consumption", "--run", RUN, lake=lake)
    job("jobs.billing", "--run", RUN, lake=lake)
    job("jobs.transformer_losses", "--run", RUN, lake=lake)
    assert "suspicious_meters: " in job("jobs.suspicious_meters", "--run", RUN, lake=lake)
    assert "OK: 21 quality checks passed" in job("jobs.quality_checks", "--run", RUN, lake=lake)   # T-49.3


def test_a_wrong_argument_stops_the_job_with_an_error(lake):
    done = subprocess.run([sys.executable, "-m", "jobs.estimate_gaps", "--run", "f3-1999-01-01", "--lake", str(lake)],
                          cwd=REPO, capture_output=True, text=True, timeout=600)
    assert done.returncode != 0
    assert "run manifest not found" in done.stdout + done.stderr


def test_bad_data_stops_the_pipeline(lake, spark):
    """T-49.3: a negative reading in filled (written by hand, after the run above) makes the quality job fail."""
    from jobs.estimate_gaps import filled_path
    from jobs.lake import Lake

    path = filled_path(Lake.local(str(lake)))
    day = spark.read.parquet(path).filter("date = '2013-03-05'")
    broken = day.withColumn("energy_kwh", day.energy_kwh * -1).localCheckpoint(eager=True)
    broken.write.mode("overwrite").partitionBy("date").parquet(path)
    done = subprocess.run([sys.executable, "-m", "jobs.quality_checks", "--run", RUN, "--lake", str(lake)],
                          cwd=REPO, capture_output=True, text=True, timeout=600)
    assert done.returncode == 1
    failed = [line for line in done.stdout.splitlines() if line.startswith("FAIL")]
    assert len(failed) == 1 and "filled" in failed[0] and "energy in 0..25 kWh" in failed[0]
    assert "QUALITY FAILED" in done.stdout
