"""Monthly billing check (US-36, RN-04): share of estimated readings per customer and month (T-36.1).

    python -m jobs.billing --month 2013-01 --lake ../voltiagrid-api/lake

Input: curated/dataset=fact_consumption (US-35), which already counts intervals and estimated intervals per band.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def estimated_share(fact: DataFrame) -> DataFrame:
    """One row per customer and month: intervals, estimated intervals and estimated_pct (0.12 = 12%).

    It counts intervals, not kWh: RN-04 talks about estimated readings. Intervals marked 'unavailable' (no data to
    estimate from) count as estimated.
    """
    return (
        fact.groupBy("customer_code", "month")
        .agg(F.sum("intervals").alias("intervals"), F.sum("estimated_intervals").alias("estimated_intervals"))
        .withColumn("estimated_pct", F.round(F.col("estimated_intervals") / F.col("intervals"), 4))
    )
