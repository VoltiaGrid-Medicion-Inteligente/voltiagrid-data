"""Publishing the settled events to the API database (US-44, T-44.4)."""

from datetime import date, datetime

from jobs.lake import Lake

DAY = date(2013, 7, 22)


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params):
        self.log.append(("execute", sql, params))

    def executemany(self, sql, rows):
        self.log.append(("executemany", sql, list(rows)))


class FakeConnection:
    def __init__(self):
        self.log, self.commits = [], 0

    def cursor(self):
        return FakeCursor(self.log)

    def commit(self):
        self.commits += 1


def build_lake(spark, tmp_path):
    from jobs.dr_events import EVENT_SCHEMA
    from jobs.dr_settlement import settlement_path

    lake = Lake.local(str(tmp_path / "lake"))
    spark.createDataFrame([("DR-CT-1-20130722", "CT-1", datetime(2013, 7, 22, 19), datetime(2013, 7, 22, 22), 3.0,
                            31.0, 0.9, DAY)], EVENT_SCHEMA).write.partitionBy("date").parquet(lake.curated("dr_events"))
    spark.createDataFrame(
        [("DR-CT-1-20130722", "CU-1", 5, 7.2, 6.0, 1.2, 0.1667, True, 0.3, 0.36, DAY),
         ("DR-CT-1-20130722", "CU-9", 2, None, 5.0, None, None, False, 0.3, 0.0, DAY)],
        "event_id string, customer_code string, baseline_days int, baseline_kwh double, actual_kwh double, "
        "savings_kwh double, savings_pct double, incentive_paid boolean, price_per_kwh double, "
        "incentive_amount double, date date").write.partitionBy("date").parquet(settlement_path(lake))
    return lake


def test_rows_of_the_days_are_replaced_in_one_transaction(spark, tmp_path):
    from jobs.publish_dr import DELETE_SQL, INSERT_SQL, run

    connection = FakeConnection()
    assert run(spark, build_lake(spark, tmp_path), [DAY], connection) == 2
    (kind1, sql1, params1), (kind2, sql2, rows) = connection.log
    assert (kind1, sql1, params1) == ("execute", DELETE_SQL, ([DAY],))
    assert (kind2, sql2) == ("executemany", INSERT_SQL)
    assert rows[0] == ("DR-CT-1-20130722", "CU-1", DAY, datetime(2013, 7, 22, 19), datetime(2013, 7, 22, 22), 7.2, 6.0,
                       1.2, 0.1667, True, 0.36)
    assert rows[1][1] == "CU-9" and rows[1][5] is None              # no baseline: stored as NULL
    assert connection.commits == 1


def test_a_day_without_events_only_clears_the_day(spark, tmp_path):
    from jobs.publish_dr import run

    connection = FakeConnection()
    assert run(spark, build_lake(spark, tmp_path), [date(2013, 7, 23)], connection) == 0
    assert [entry[0] for entry in connection.log] == ["execute"]
    assert connection.commits == 1


def test_a_lake_without_settled_events_only_clears_the_day(spark, tmp_path):
    from jobs.publish_dr import run

    connection = FakeConnection()
    assert run(spark, Lake.local(str(tmp_path / "empty")), [DAY], connection) == 0
    assert [entry[0] for entry in connection.log] == ["execute"]