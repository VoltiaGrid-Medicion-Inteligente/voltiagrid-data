# schemas/f1.py
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Schema for the halfhourly_dataset (blocks 0 to 111)
f1_schema = StructType([
    StructField("LCLid", StringType(), True),
    StructField("tstp", TimestampType(), True),
    # Leemos la energía como String porque sabemos que tiene defectos con el texto "Null"
    StructField("energy(kWh/hh)", StringType(), True) 
])

# Schema for informations_households.csv
households_schema = StructType([
    StructField("LCLid", StringType(), True),
    StructField("stdorToU", StringType(), True),
    StructField("Acorn", StringType(), True),
    StructField("Acorn_grouped", StringType(), True),
    StructField("file", StringType(), True)
])