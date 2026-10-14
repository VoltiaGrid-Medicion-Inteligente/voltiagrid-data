
from pyspark.sql.types import StringType, StructField, StructType, TimestampNTZType

F1_SCHEMA = StructType([
    StructField("LCLid", StringType(), True),
    StructField("tstp", TimestampNTZType(), True),
    StructField("energy(kWh/hh)", StringType(), True),
])

HOUSEHOLDS_SCHEMA = StructType([
    StructField("LCLid", StringType(), True),
    StructField("stdorToU", StringType(), True),
    StructField("Acorn", StringType(), True),
    StructField("Acorn_grouped", StringType(), True),
    StructField("file", StringType(), True),
])