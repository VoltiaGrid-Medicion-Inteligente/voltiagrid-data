"""Loss-injection scenario (US-62, T-62.1): the F5 injection contract, cleanup, and the check of the detection."""

from datetime import date

import pytest

from jobs.lake import Lake
from jobs.loss_injection_scenario import check_scenario, inject, injection_body, remove, run_scenario

LAST = date(2013, 1, 19)
SUSPICIOUS_SCHEMA = "transformer_code string, date date"
EVALUATED_SCHEMA = "meter_id string, transformer_code string, date date"


def test_injection_body_is_the_f5_contract():
    body = injection_body("TR-00001", date(2013, 1, 17), 3, 0.15)
    assert body == {"transformer_code": "TR-00001", "start": "2013-01-17T00:00:00Z", "end": "2013-01-20T00:00:00Z",
                    "loss_pct": 0.15}


def test_inject_posts_the_body_and_returns_the_id():
    posted = []

    def post(url, body):
        posted.append((url, body))
        return {"id": 7}

    body = injection_body("TR-00001", date(2013, 1, 17), 3, 0.15)
    assert inject("http://f5", body, post=post) == 7
    assert posted == [("http://f5/losses", body)]


def test_remove_deletes_the_injection_by_id():
    deleted = []
    remove("http://f5", 7, delete=deleted.append)
    assert deleted == ["http://f5/losses/7"]


def test_injections_are_removed_even_when_detection_fails(spark, tmp_path):
    lake = Lake.local(str(tmp_path / "lake"))
    posted_ids, deleted = iter([11, 12]), []

    def post(url, body):
        return {"id": next(posted_ids)}

    def failing_detection(spark_, lake_, days):
        raise RuntimeError("detection broke")

    with pytest.raises(RuntimeError):
        run_scenario(spark, lake, "http://f5", ["TR-00001", "TR-00002"], date(2013, 1, 17), 3, 0.15,
                     post=post, delete=deleted.append, get=lambda url: b"[]", detect_fn=failing_detection)
    assert deleted == ["http://f5/losses/11", "http://f5/losses/12"]


def test_check_passes_when_injected_transformer_is_suspicious_and_its_meters_evaluated(spark):
    suspicious = spark.createDataFrame([("TR-00001", LAST)], SUSPICIOUS_SCHEMA)
    evaluated = spark.createDataFrame([("M-1", "TR-00001", LAST), ("M-2", "TR-00001", LAST)], EVALUATED_SCHEMA)
    assert check_scenario(suspicious, evaluated, ["TR-00001"], LAST) == []


def test_check_reports_a_transformer_that_was_not_flagged(spark):
    suspicious = spark.createDataFrame([("TR-00002", LAST)], SUSPICIOUS_SCHEMA)
    evaluated = spark.createDataFrame([("M-1", "TR-00001", LAST)], EVALUATED_SCHEMA)
    problems = check_scenario(suspicious, evaluated, ["TR-00001"], LAST)
    assert problems == ["TR-00001 is not suspicious on 2013-01-19"]  # its meter was evaluated, so only the flag fails


def test_check_only_looks_at_the_last_day(spark):
    suspicious = spark.createDataFrame([("TR-00001", date(2013, 1, 17))], SUSPICIOUS_SCHEMA)
    evaluated = spark.createDataFrame([("M-1", "TR-00001", LAST)], EVALUATED_SCHEMA)
    assert check_scenario(suspicious, evaluated, ["TR-00001"], LAST) == ["TR-00001 is not suspicious on 2013-01-19"]


def test_a_meter_of_another_transformer_does_not_count(spark):
    suspicious = spark.createDataFrame([("TR-00001", LAST)], SUSPICIOUS_SCHEMA)
    evaluated = spark.createDataFrame([("M-9", "TR-00002", LAST)], EVALUATED_SCHEMA)
    assert check_scenario(suspicious, evaluated, ["TR-00001"], LAST) == ["no meter of TR-00001 was evaluated on 2013-01-19"]
