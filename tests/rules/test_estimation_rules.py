"""pipeline.estimation beyond the CA-03 scenario (RN-03)."""

from datetime import datetime, timedelta

from pipeline.estimation import fill_gaps


def test_estimation_interpolates_on_the_line_and_marks_unavailable_without_history(make_reading):
    t0 = datetime(2013, 3, 4)
    series = [make_reading(ts=t0, kwh=0.2), make_reading(ts=t0 + timedelta(hours=2), kwh=0.6)]  # 3 missing between
    series += [make_reading(ts=t0 + timedelta(hours=6), kwh=0.1)]                               # 7 missing between
    rows = {r["ts"]: r for r in fill_gaps(series)}
    assert [rows[t0 + timedelta(minutes=30 * i)]["energy_kwh"] for i in (1, 2, 3)] == [0.3, 0.4, 0.5]
    assert rows[t0 + timedelta(hours=3)]["estimation_method"] == "unavailable"
    assert len(rows) == 13
