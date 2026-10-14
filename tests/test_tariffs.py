import shutil
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from validate_tariffs import (
    DEFAULT_CSV,
    MAX_ROWS,
    MIN_PERIODS,
    REQUIRED_BANDS,
    band_for,
    load_tariffs,
    validation_errors,
)

CSV_PATH = Path(__file__).resolve().parent.parent / DEFAULT_CSV


@pytest.fixture(scope="module")
def rows():
    return load_tariffs(CSV_PATH)


def test_calendar_has_no_validation_errors(rows):
    assert validation_errors(rows) == []


def test_fewer_than_100_rows(rows):
    assert len(rows) < MAX_ROWS


def test_at_least_two_validity_periods(rows):
    assert len({r.period for r in rows}) >= MIN_PERIODS


def test_has_peak_intermediate_and_off_peak(rows):
    assert {r.band_name for r in rows} == REQUIRED_BANDS


def test_every_band_has_a_positive_price(rows):
    assert all(r.price_per_kwh > 0 for r in rows)


def test_spark_reads_csv_with_explicit_schema():
    pyspark = pytest.importorskip("pyspark")
    if shutil.which("java") is None:
        pytest.skip("Java is not installed")
    from schemas.f7 import F7_SCHEMA

    spark = (
        pyspark.sql.SparkSession.builder.master("local[1]")
        .appName("test-f7-schema")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        df = spark.read.csv(str(CSV_PATH), header=True, schema=F7_SCHEMA, mode="FAILFAST")
        data = df.collect()
        loaded_types = [(f.name, f.dataType) for f in df.schema.fields]
    finally:
        spark.stop()

    assert loaded_types == [(f.name, f.dataType) for f in F7_SCHEMA.fields]
    assert len(data) == len(load_tariffs(CSV_PATH))
    assert all(isinstance(r["price_per_kwh"], Decimal) for r in data)
    assert all(r[c] is not None for r in data for c in F7_SCHEMA.fieldNames())


def test_exactly_one_band_for_every_half_hour_of_the_dataset(rows):
    ts = datetime(2011, 11, 1)
    end = datetime(2014, 4, 1)
    while ts < end:
        band_for(rows, ts)  
        ts += timedelta(minutes=30)


@pytest.mark.parametrize(
    "ts, band, price",
    [
        ("2011-12-31 07:59:59", "off-peak", "0.10"),
        ("2011-12-31 08:00:00", "intermediate", "0.15"),
        ("2011-12-31 21:59:59", "peak", "0.25"),
        ("2011-12-31 23:59:59", "off-peak", "0.10"),
        ("2012-01-01 00:00:00", "off-peak", "0.12"),
        ("2013-07-15 18:30:00", "peak", "0.30"),
    ],
)
def test_boundaries_get_the_expected_band(rows, ts, band, price):
    row = band_for(rows, datetime.fromisoformat(ts))
    assert (row.band_name, row.price_per_kwh) == (band, Decimal(price))

HEADER = "valid_from,valid_to,start_time,end_time,band_name,price_per_kwh\n"


def write_calendar(tmp_path, body: str) -> Path:
    path = tmp_path / "tariffs.csv"
    path.write_text(HEADER + body, encoding="utf-8")
    return path


def day(valid_from: str, valid_to: str, price: str = "0.10") -> str:
    return (
        f"{valid_from},{valid_to},00:00:00,07:59:59,off-peak,{price}\n"
        f"{valid_from},{valid_to},08:00:00,17:59:59,intermediate,{price}\n"
        f"{valid_from},{valid_to},18:00:00,23:59:59,peak,{price}\n"
    )


def test_overlapping_periods_are_rejected(tmp_path):
    path = write_calendar(tmp_path, day("2011-01-01", "2012-06-30") + day("2012-01-01", "2099-12-31"))
    errors = validation_errors(load_tariffs(path))
    assert any("periods overlap" in e for e in errors)


def test_gap_between_hours_is_rejected(tmp_path):
    body = day("2011-01-01", "2011-12-31").replace("08:00:00", "09:00:00") + day("2012-01-01", "2099-12-31")
    errors = validation_errors(load_tariffs(write_calendar(tmp_path, body)))
    assert any("gap between" in e for e in errors)


def test_zero_price_is_rejected(tmp_path):
    path = write_calendar(tmp_path, day("2011-01-01", "2011-12-31", "0") + day("2012-01-01", "2099-12-31"))
    assert any("price_per_kwh must be > 0" in e for e in validation_errors(load_tariffs(path)))


def test_lookup_fails_when_two_bands_apply(tmp_path):
    path = write_calendar(tmp_path, day("2011-01-01", "2012-06-30") + day("2012-01-01", "2099-12-31"))
    with pytest.raises(LookupError, match="2 bands apply"):
        band_for(load_tariffs(path), datetime(2012, 3, 1, 12, 0))