from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

F7_SCHEMA = StructType([
    StructField("valid_from", DateType(), False),
    StructField("valid_to", DateType(), False),
    StructField("start_time", StringType(), False),
    StructField("end_time", StringType(), False),
    StructField("band_name", StringType(), False),
    StructField("price_per_kwh", DecimalType(10, 4), False),
])