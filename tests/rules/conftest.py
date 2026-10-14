from datetime import datetime, timedelta

import pytest

START = datetime(2013, 3, 4, 0, 0)  # a Monday
HALF_HOUR = timedelta(minutes=30)


@pytest.fixture
def make_reading():
    def _make(meter="MAC000002", ts=START, kwh=0.25, seq=1):
        return {"meter_id": meter, "ts": ts, "energy_kwh": kwh, "seq": seq}

    return _make


@pytest.fixture
def duplicated_interval(make_reading):
    return [make_reading(kwh=0.20, seq=1), make_reading(kwh=0.30, seq=2)]


@pytest.fixture
def series_with_gaps(make_reading):
    history = [make_reading(ts=START + i * HALF_HOUR, kwh=0.20) for i in range(4 * 7 * 48)]
    last_day = START + timedelta(days=28)
    day = [make_reading(ts=last_day + i * HALF_HOUR, kwh=0.30) for i in range(48)]
    gap_90_min = set(range(10, 13))  
    gap_5_hours = set(range(30, 40))  
    day = [r for i, r in enumerate(day) if i not in gap_90_min | gap_5_hours]
    return history + day


@pytest.fixture
def transformer_losses():
    return {"transformer": "TR-00001", "daily_loss_pct": [0.05, 0.15, 0.15, 0.15]}