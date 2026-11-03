"""F3 message as stored in raw by the consumer (voltiagrid-api, reading_batch.v1.json).

energy_kwh is read as text on purpose: the source sends a number, null or the text "Null" (defects of US-02), and a
numeric schema would turn the text into null without saying so. The cleaning job decides what each value means.
interval_start is text too: it carries a time zone ("Z") that a TimestampNTZ field would not accept.
"""

from pyspark.sql.types import ArrayType, BooleanType, LongType, StringType, StructField, StructType

F3_READING_SCHEMA = StructType([
    StructField("meter_id", StringType(), True),
    StructField("interval_start", StringType(), True),
    StructField("energy_kwh", StringType(), True),
    StructField("reading_seq", LongType(), True),
])

F3_MESSAGE_SCHEMA = StructType([
    StructField("schema_version", StringType(), True),
    StructField("message_id", StringType(), True),
    StructField("concentrator_id", StringType(), True),
    StructField("sequence", LongType(), True),
    StructField("sent_at", StringType(), True),
    StructField("is_resend", BooleanType(), True),
    StructField("readings", ArrayType(F3_READING_SCHEMA), True),
])
