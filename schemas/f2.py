# schemas/f2.py
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

# Schema for weather_hourly_darksky.csv
f2_schema = StructType([
    StructField("visibility", DoubleType(), True),
    StructField("windBearing", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("time", TimestampType(), True),
    StructField("icon", StringType(), True),
    StructField("windDegree", DoubleType(), True),
    StructField("windSpeed", DoubleType(), True),
    StructField("precipType", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("apparentTemperature", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("precipProbability", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("uvIndex", DoubleType(), True),
    StructField("precipIntensity", DoubleType(), True),
    StructField("dewPoint", DoubleType(), True)
])