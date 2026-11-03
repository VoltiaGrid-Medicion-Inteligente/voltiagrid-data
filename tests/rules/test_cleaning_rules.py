"""pipeline.cleaning beyond the two CA scenarios (RN-01, RN-02)."""

from datetime import datetime

import pytest

from pipeline.cleaning import deduplicate, normalize_clock


@pytest.mark.parametrize("ts, expected", [
    (datetime(2013, 3, 4, 10, 35), (datetime(2013, 3, 4, 10, 30), False)),
    (datetime(2013, 3, 4, 10, 35, 1), (None, True)),
    (datetime(2013, 3, 4, 10, 25), (datetime(2013, 3, 4, 10, 30), False)),
    (datetime(2013, 3, 4, 23, 57), (datetime(2013, 3, 5, 0, 0), False)),
    (datetime(2013, 3, 4, 10, 0), (datetime(2013, 3, 4, 10, 0), False)),
])
def test_normalize_clock_edges(ts, expected):
    assert normalize_clock(ts) == expected


def test_deduplicate_groups_by_normalized_interval_and_keeps_invalid_clocks(make_reading):
    readings = [
        make_reading(ts=datetime(2013, 3, 4, 10, 28), kwh=0.4, seq=5),
        make_reading(ts=datetime(2013, 3, 4, 10, 31), kwh=0.1, seq=3),
        make_reading(ts=datetime(2013, 3, 4, 10, 41), kwh=0.2, seq=9),
    ]
    result = deduplicate(readings)
    assert sorted(r["seq"] for r in result) == [5, 9]
