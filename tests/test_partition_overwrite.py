"""A reprocessed day whose new result is empty must not keep the old partition (KAN-331 rehearsal, 12 Nov 2026)."""
from datetime import date

from pyspark.sql import Row

from jobs.transformer_losses import _write_by_date


def _rows(spark, path, day):
    return spark.read.parquet(path).filter(f"date = '{day}'").count()


def test_an_empty_result_clears_the_old_partition_of_that_day(spark, tmp_path):
    path = str(tmp_path / "suspicious_transformers")
    old = spark.createDataFrame([Row(transformer_code="TR-00001", rank=1, date="2013-01-04"),
                                 Row(transformer_code="TR-00002", rank=1, date="2013-01-05")])
    _write_by_date(spark, old, path, [date(2013, 1, 4), date(2013, 1, 5)])
    assert _rows(spark, path, "2013-01-04") == 1

    empty = old.limit(0)
    _write_by_date(spark, empty, path, [date(2013, 1, 4)])

    assert _rows(spark, path, "2013-01-04") == 0
    assert _rows(spark, path, "2013-01-05") == 1      # days not reprocessed stay as they were


def test_a_new_result_replaces_the_old_one(spark, tmp_path):
    path = str(tmp_path / "losses")
    _write_by_date(spark, spark.createDataFrame([Row(transformer_code="TR-00001", loss_pct=0.15, date="2013-01-04")]),
                   path, [date(2013, 1, 4)])
    _write_by_date(spark, spark.createDataFrame([Row(transformer_code="TR-00001", loss_pct=0.01, date="2013-01-04")]),
                   path, [date(2013, 1, 4)])
    rows = spark.read.parquet(path).collect()
    assert [r.loss_pct for r in rows] == [0.01]
