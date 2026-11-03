"""Raw readings read with an explicit schema (US-30, T-30.1)."""

import json
from datetime import datetime

import pytest

from jobs.readings import READING_COLUMNS, read_f1, read_f3


def write_f3(folder, sequence, readings):
    folder.mkdir(parents=True, exist_ok=True)
    message = {
        "schema_version": "1.0", "message_id": f"DC-TR-00001-{sequence:08d}", "concentrator_id": "DC-TR-00001",
        "sequence": sequence, "sent_at": "2013-01-01T01:00:00Z", "is_resend": False, "readings": readings,
    }
    (folder / f"DC-TR-00001_{sequence:08d}.json").write_text(json.dumps(message), encoding="utf-8")


def reading(meter="MAC000002", ts="2013-01-01T00:30:00Z", kwh=0.25, seq=1):
    return {"meter_id": meter, "interval_start": ts, "energy_kwh": kwh, "reading_seq": seq}


@pytest.fixture
def raw_f3(tmp_path):
    day = tmp_path / "source=f3" / "dataset=readings" / "ingest_date=2026-11-02"
    write_f3(day / "ingest_hour=14", 1, [reading(), reading(meter="MAC000003", kwh=None, seq=2)])
    write_f3(day / "ingest_hour=15", 2, [reading(kwh="Null", seq=3), reading(ts="2013-01-01T00:33:00+00:00", kwh=-1.5, seq=4)])
    return day


def test_f3_messages_become_one_row_per_reading_with_the_common_columns(spark, raw_f3):
    df = read_f3(spark, [str(raw_f3)])
    assert df.columns == READING_COLUMNS
    rows = sorted(df.collect(), key=lambda r: r.seq)
    assert len(rows) == 4  # both hour folders are read
    assert rows[0].original_ts == datetime(2013, 1, 1, 0, 30)
    assert rows[3].original_ts == datetime(2013, 1, 1, 0, 33)  # "+00:00" and "Z" give the same UTC time
    assert {r.source for r in rows} == {"f3"}


def test_f3_energy_is_kept_as_text_so_no_defect_is_hidden(spark, raw_f3):
    energies = {r.seq: r.energy_text for r in read_f3(spark, [str(raw_f3)]).collect()}
    assert energies == {1: "0.25", 2: None, 3: "Null", 4: "-1.5"}


def test_f3_types_come_from_the_schema_not_from_the_data(spark, raw_f3):
    types = dict(read_f3(spark, [str(raw_f3)]).dtypes)
    assert types == {"meter_id": "string", "original_ts": "timestamp_ntz", "energy_text": "string",
                     "seq": "bigint", "source": "string"}


def test_f1_kaggle_block_with_seven_decimal_seconds_and_null_text(spark, tmp_path):
    block = tmp_path / "block_0.csv"
    block.write_text(
        "LCLid,tstp,energy(kWh/hh)\n"
        "MAC000002,2012-10-12 00:30:00.0000000, 0 \n"
        "MAC000002,2012-10-12 01:00:00.0000000,Null\n",
        encoding="utf-8",
    )
    rows = read_f1(spark, [str(block)]).collect()
    assert [r.original_ts for r in rows] == [datetime(2012, 10, 12, 0, 30), datetime(2012, 10, 12, 1, 0)]
    assert [r.energy_text for r in rows] == ["0", "Null"]
    assert {(r.seq, r.source) for r in rows} == {(0, "f1")}


def test_f1_file_with_other_columns_fails_instead_of_being_guessed(spark, tmp_path):
    block = tmp_path / "block_1.csv"
    block.write_text("id,time,kwh\nMAC000002,2012-10-12 00:30:00,0.1\n", encoding="utf-8")
    with pytest.raises(Exception, match="(?i)header|schema|LCLid"):
        read_f1(spark, [str(block)]).collect()
