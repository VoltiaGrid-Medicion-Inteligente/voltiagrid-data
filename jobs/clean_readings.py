"""Clean readings: raw -> clean (US-30). Built in steps: clock (T-30.2), duplicates (T-30.3), quality (T-30.4)."""

from pyspark.sql import DataFrame
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
