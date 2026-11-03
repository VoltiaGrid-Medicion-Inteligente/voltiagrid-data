"""F4 inventory exported to raw and read by the jobs (US-33, T-33.2)."""

import os
from datetime import date
from decimal import Decimal

import pytest

from jobs.inventory import circuit_capacity, psycopg_url, read_contracts, read_meters, write_raw
from jobs.lake import Lake

METERS = (["meter_id", "customer_code", "transformer_code", "transformer_capacity_kva", "circuit_code",
           "circuit_capacity_kva"],
          [("MAC000002", "CU-MAC000002", "TR-00000", Decimal("141.00"), "CT-000", Decimal("2100.12")),
           ("MAC000003", "CU-MAC000003", "TR-00000", Decimal("141.00"), "CT-000", Decimal("2100.12")),
           ("MAC000004", "CU-MAC000004", "TR-00001", Decimal("300.50"), "CT-001", Decimal("1500.00"))])
CONTRACTS = (["customer_code", "contract_code", "tariff_type", "start_date", "end_date"],
             [("CU-MAC000002", "CT-MAC000002-1", "standard", date(2011, 11, 1), None)])


@pytest.fixture
def lake(tmp_path):
    lake = Lake.local(str(tmp_path / "lake"))
    write_raw(lake, {"meters": METERS, "contracts": CONTRACTS}, date(2026, 11, 1))
    return lake


def test_export_writes_csv_and_manifest_in_the_raw_partition(lake, tmp_path):
    folder = tmp_path / "lake" / "raw" / "source=f4" / "dataset=meters" / "ingest_date=2026-11-01"
    assert (folder / "meters.csv").read_text().splitlines()[0] == ",".join(METERS[0])
    assert '"rows": 3' in (folder / "_manifest.json").read_text()


def test_readers_use_the_explicit_schema(spark, lake):
    meters = read_meters(spark, lake)
    assert dict(meters.dtypes)["circuit_capacity_kva"] == "decimal(8,2)"
    assert meters.count() == 3
    contract = read_contracts(spark, lake).first()
    assert contract.start_date == date(2011, 11, 1) and contract.end_date is None


def test_readers_take_the_latest_export(spark, lake):
    newer = (METERS[0], METERS[1][:1])
    write_raw(lake, {"meters": newer}, date(2026, 11, 5))
    assert read_meters(spark, lake).count() == 1
    assert read_meters(spark, lake, ingest_date="2026-11-01").count() == 3


def test_circuit_capacity_comes_from_the_inventory(spark, lake):
    rows = {r.circuit_code: r for r in circuit_capacity(read_meters(spark, lake)).collect()}
    assert rows["CT-000"].capacity_kva == Decimal("2100.12")
    assert (rows["CT-000"].transformers, rows["CT-000"].meters) == (1, 2)
    assert rows["CT-001"].capacity_kva == Decimal("1500.00")


def test_a_circuit_with_two_capacities_is_an_error(spark, tmp_path):
    lake = Lake.local(str(tmp_path / "lake2"))
    rows = METERS[1][:2] + [("MAC000009", "CU-MAC000009", "TR-00009", Decimal("100"), "CT-000", Decimal("999"))]
    write_raw(lake, {"meters": (METERS[0], rows)}, date(2026, 11, 1))
    with pytest.raises(ValueError, match="CT-000"):
        circuit_capacity(read_meters(spark, lake))


def test_missing_export_says_what_to_run(spark, tmp_path):
    with pytest.raises(FileNotFoundError, match="jobs.inventory export"):
        read_meters(spark, Lake.local(str(tmp_path / "empty")))


def test_sqlalchemy_url_of_the_api_is_accepted():
    assert psycopg_url("postgresql+psycopg://u:p@localhost:5433/voltiagrid") == "postgresql://u:p@localhost:5433/voltiagrid"


@pytest.mark.skipif(not os.getenv("F4_TEST_DATABASE_URL"), reason="F4_TEST_DATABASE_URL not set (seeded API database)")
def test_export_from_a_seeded_api_database(spark, tmp_path):
    from jobs.inventory import fetch

    lake = Lake.local(str(tmp_path / "lake"))
    tables = fetch(os.environ["F4_TEST_DATABASE_URL"])
    write_raw(lake, tables, date(2026, 11, 1))
    meters = read_meters(spark, lake)
    assert meters.count() == len(tables["meters"][1]) > 0
    assert read_contracts(spark, lake).count() >= 1
