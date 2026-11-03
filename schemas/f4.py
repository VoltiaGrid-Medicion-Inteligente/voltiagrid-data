"""F4 network inventory as exported to raw by jobs/inventory.py (source: voltiagrid-api database, docs/erd.md)."""

from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

F4_METERS_SCHEMA = StructType([
    StructField("meter_id", StringType(), False),
    StructField("customer_code", StringType(), False),
    StructField("transformer_code", StringType(), False),
    StructField("transformer_capacity_kva", DecimalType(8, 2), False),
    StructField("circuit_code", StringType(), False),
    StructField("circuit_capacity_kva", DecimalType(8, 2), False),
])

F4_CONTRACTS_SCHEMA = StructType([
    StructField("customer_code", StringType(), False),
    StructField("contract_code", StringType(), False),
    StructField("tariff_type", StringType(), False),
    StructField("start_date", DateType(), False),
    StructField("end_date", DateType(), True),
])
