"""The quarterly report agrees with the losses fact (US-54, T-54.3): a report that differs must be caught."""

from pyspark.sql import functions as F

from jobs.quarterly_report import build_report, check_against_fact
from tests.test_quarterly_report import build_lake, q1_fact


def test_report_built_from_the_fact_matches_it(spark, tmp_path):
    lake = build_lake(spark, tmp_path)
    fact = q1_fact(spark, lake)
    report = build_report(spark, lake, "2013-Q1", fact)
    assert check_against_fact(report, fact) == []


def test_a_changed_loss_is_reported_by_transformer(spark, tmp_path):
    lake = build_lake(spark, tmp_path)
    fact = q1_fact(spark, lake)
    report = build_report(spark, lake, "2013-Q1", fact)
    tampered = report.withColumn("loss_kwh", F.when(F.col("transformer_code") == "TR-1", F.col("loss_kwh") + 0.5)
                                 .otherwise(F.col("loss_kwh")))
    assert check_against_fact(tampered, fact) == ["TR-1"]


def test_a_transformer_missing_from_the_report_is_reported(spark, tmp_path):
    lake = build_lake(spark, tmp_path)
    fact = q1_fact(spark, lake)
    report = build_report(spark, lake, "2013-Q1", fact).filter("transformer_code <> 'TR-2'")
    assert check_against_fact(report, fact) == ["TR-2"]


def test_a_transformer_missing_from_the_fact_is_reported(spark, tmp_path):
    lake = build_lake(spark, tmp_path)
    fact = q1_fact(spark, lake)
    report = build_report(spark, lake, "2013-Q1", fact)
    fact_without_tr2 = fact.filter("transformer_code <> 'TR-2'")
    assert check_against_fact(report, fact_without_tr2) == ["TR-2"]
