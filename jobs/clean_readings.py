"""Clean readings: raw -> clean (US-30). Built in steps: clock (T-30.2), duplicates (T-30.3), quality (T-30.4)."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

HALF_HOUR_SECONDS = 30 * 60
CLOCK_TOLERANCE_SECONDS = 5 * 60


def normalize_clock(df: DataFrame) -> DataFrame:
    """RN-01: move each reading to the nearest :00 or :30 if it is at most 5 minutes away; otherwise invalid clock.

    Adds:
    - interval_start (timestamp_ntz): the normalized interval, null when the clock is invalid.
    - clock_ok (boolean).
    10:33 -> 10:30; 10:27 -> 10:30; 23:57 -> 00:00 of the next day; 10:41 -> invalid (11 minutes from 10:30).
    """
    seconds = F.unix_timestamp(F.col("original_ts").cast("timestamp"))  # session time zone is UTC
    offset = F.pmod(seconds, F.lit(HALF_HOUR_SECONDS))
    nearest = (
        F.when(offset <= CLOCK_TOLERANCE_SECONDS, seconds - offset)
        .when(offset >= HALF_HOUR_SECONDS - CLOCK_TOLERANCE_SECONDS, seconds - offset + HALF_HOUR_SECONDS)
    )
    return (
        df.withColumn("interval_start", F.timestamp_seconds(nearest).cast("timestamp_ntz"))
        .withColumn("clock_ok", F.col("interval_start").isNotNull())
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Two passes, after normalize_clock:

    1. Exact copies (same meter, source, original time, sequence and value) become one row. They come from redeliveries:
       a message stored twice after a consumer crash (ADR-007), or the same F1 block read twice.
    2. RN-02: among the readings of one meter that fall in the same interval, the highest sequence wins, whatever
       its value (a null with sequence 5 beats 0.30 with sequence 3; the quality step flags it later).
       Ties (F1 has no sequence) are broken by the latest original time, then the highest text value, so the
       result does not depend on how Spark split the data.
    Readings with an invalid clock have no interval: they skip pass 2 and stay, flagged, for the quality report.
    """
    exact = df.dropDuplicates(["meter_id", "source", "original_ts", "seq", "energy_text"])
    valid, invalid = exact.filter(F.col("clock_ok")), exact.filter(~F.col("clock_ok"))
    window = Window.partitionBy("meter_id", "interval_start").orderBy(
        F.col("seq").desc(), F.col("original_ts").desc(), F.col("energy_text").desc_nulls_last()
    )
    winners = (
        valid.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    return winners.unionByName(invalid)
