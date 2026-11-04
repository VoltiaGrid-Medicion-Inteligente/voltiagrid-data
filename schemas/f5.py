"""F5 transformer metering API response, stored as received in raw (voltiagrid-api, simulators/transformer_api)."""

from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType

F5_READING_SCHEMA = StructType([
    StructField("interval_start", StringType(), True),
    StructField("delivered_kwh", DoubleType(), True),
])

F5_RESPONSE_SCHEMA = StructType([
    StructField("transformer_code", StringType(), True),
    StructField("interval_minutes", IntegerType(), True),
    StructField("readings", ArrayType(F5_READING_SCHEMA), True),
])
