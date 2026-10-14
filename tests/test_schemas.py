from pathlib import Path

import pytest

from schemas.f1 import F1_SCHEMA, HOUSEHOLDS_SCHEMA
from schemas.f2 import F2_SCHEMA
from schemas.header_check import assert_header, header_mismatch

REAL_HEADERS = {
    "block_0.csv": "LCLid,tstp,energy(kWh/hh)",
    "informations_households.csv": "LCLid,stdorToU,Acorn,Acorn_grouped,file",
    "weather_hourly_darksky.csv": (
        "visibility,windBearing,temperature,time,dewPoint,pressure,"
        "apparentTemperature,windSpeed,precipType,icon,humidity,summary"
    ),
}


def write_csv(tmp_path: Path, name: str, header: str) -> Path:
    path = tmp_path / name
    path.write_text(header + "\n", encoding="utf-8")
    return path


@pytest.mark.parametrize(
    "name, schema",
    [
        ("block_0.csv", F1_SCHEMA),
        ("informations_households.csv", HOUSEHOLDS_SCHEMA),
        ("weather_hourly_darksky.csv", F2_SCHEMA),
    ],
)
def test_schema_matches_real_header(tmp_path, name, schema):
    path = write_csv(tmp_path, name, REAL_HEADERS[name])
    assert header_mismatch(path, schema.fieldNames()) is None


def test_wrong_column_order_is_rejected(tmp_path):
    path = write_csv(tmp_path, "weather_hourly_darksky.csv", "time,visibility")
    with pytest.raises(ValueError, match="does not match schema"):
        assert_header(path, ["visibility", "time"])


def test_header_with_bom_is_accepted(tmp_path):
    path = tmp_path / "block_0.csv"
    path.write_text("﻿" + REAL_HEADERS["block_0.csv"] + "\n", encoding="utf-8")
    assert header_mismatch(path, F1_SCHEMA.fieldNames()) is None