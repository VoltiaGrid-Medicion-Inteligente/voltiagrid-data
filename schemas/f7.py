from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Esquema para el calendario tarifario F7
f7_schema = StructType([
    StructField("valid_from", StringType(), False),
    StructField("valid_to", StringType(), False),
    StructField("start_time", StringType(), False),
    StructField("end_time", StringType(), False),
    StructField("band", StringType(), False),
    StructField("price_kwh", DoubleType(), False)
])