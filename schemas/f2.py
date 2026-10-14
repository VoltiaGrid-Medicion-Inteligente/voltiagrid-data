from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampNTZType

F2_SCHEMA = StructType([
    StructField("visibility", DoubleType(), True),
    StructField("windBearing", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("time", TimestampNTZType(), True),
    StructField("dewPoint", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("apparentTemperature", DoubleType(), True),
    StructField("windSpeed", DoubleType(), True),
    StructField("precipType", StringType(), True),
    StructField("icon", StringType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("summary", StringType(), True),
])