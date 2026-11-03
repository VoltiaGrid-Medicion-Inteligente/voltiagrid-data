"""Read raw readings with an explicit schema (US-30, T-30.1).

Both sources end in the same columns, so the cleaning rules are written once:

| Column | Type | F1 (Kaggle CSV) | F3 (concentrator JSON) |
|---|---|---|---|
| meter_id | string | LCLid | readings[].meter_id |
| original_ts | timestamp_ntz (UTC) | tstp | readings[].interval_start |
| energy_text | string, as sent | energy(kWh/hh) | readings[].energy_kwh |
| seq | long | 0 (F1 has no sequence) | readings[].reading_seq |
| source | string | "f1" | "f3" |

Nothing is inferred: a file whose columns do not match the schema fails here, not three jobs later.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from schemas.f1 import F1_SCHEMA
from schemas.f3 import F3_MESSAGE_SCHEMA

READING_COLUMNS = ["meter_id", "original_ts", "energy_text", "seq", "source"]
F1_ENERGY = "energy(kWh/hh)"


def read_f1(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Kaggle half-hourly blocks (block_*.csv), header checked against F1_SCHEMA."""
    df = spark.read.csv(paths, header=True, schema=F1_SCHEMA, enforceSchema=False, mode="FAILFAST")
    return df.select(
        F.col("LCLid").alias("meter_id"),
        F.col("tstp").alias("original_ts"),
        F.trim(F.col(f"`{F1_ENERGY}`")).alias("energy_text"),
        F.lit(0).cast("long").alias("seq"),
        F.lit("f1").alias("source"),
    )


def parse_utc(column) -> F.Column:
    """ISO 8601 with offset ("2013-01-01T00:30:00Z") -> UTC wall-clock timestamp_ntz (session time zone is UTC)."""
    return F.to_timestamp(column).cast("timestamp_ntz")


def read_f3(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Concentrator messages stored by the consumer: one JSON object per file, any number of files."""
    messages = (
        spark.read.schema(F3_MESSAGE_SCHEMA)
        .option("multiLine", True)            # each file is one JSON document
        .option("recursiveFileLookup", True)  # every ingest_hour= folder under the given paths
        .option("pathGlobFilter", "*.json")
        .json(paths)
    )
    readings = messages.select(F.explode("readings").alias("r"))
    return readings.select(
        F.col("r.meter_id").alias("meter_id"),
        parse_utc(F.col("r.interval_start")).alias("original_ts"),
        F.trim(F.col("r.energy_kwh")).alias("energy_text"),
        F.col("r.reading_seq").alias("seq"),
        F.lit("f3").alias("source"),
    )

