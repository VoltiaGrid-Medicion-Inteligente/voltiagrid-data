"""Keys and checks shared by the fact tables of the analytics layer (US-48, US-51, US-45; dimensional model).

Keys are deterministic, computed from the natural values, so a fact never needs a lookup and a rerun gives the same
keys (the Data lake layout asks every job to be idempotent):
- date_key: the date as an integer yyyymmdd (20130722);
- time_key: the 30-minute slot of the day, 0 (00:00) to 47 (23:30);
- meter, customer, transformer and circuit keep their F4 codes; event_id its own code.
The dimensions of sprint 6 (US-52) use the same keys.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def date_key(column: str | Column) -> Column:
    return F.date_format(column, "yyyyMMdd").cast("int")


def time_key(column: str | Column) -> Column:
    c = F.col(column) if isinstance(column, str) else column
    return (F.hour(c) * 2 + (F.minute(c) >= 30).cast("int")).cast("int")


def orphan_keys(fact: DataFrame, dimension: DataFrame, key: str) -> list:
    """Key values of the fact that the dimension (or the F4 table that stands for it) does not have."""
    return [r[key] for r in fact.select(key).distinct().join(dimension.select(key).distinct(), key, "left_anti")
            .orderBy(key).collect()]


def check_no_orphans(fact: DataFrame, checks: dict[str, DataFrame], name: str) -> None:
    """Stop the job (SystemExit) if any key has no row on its side: a fact must never point to nothing."""
    problems = {}
    for key, dimension in checks.items():
        missing = orphan_keys(fact, dimension, key)
        if missing:
            problems[key] = missing[:10]
    if problems:
        raise SystemExit(f"{name}: orphan keys (first 10 of each): {problems}")
