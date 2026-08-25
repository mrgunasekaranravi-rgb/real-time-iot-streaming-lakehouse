# Databricks notebook source
# Real-Time IoT Streaming Lakehouse - Source Generation

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

iot_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("device_id", StringType(), False),
    StructField("sensor_type", StringType(), False),
    StructField("temperature", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("event_timestamp", TimestampType(), False),
    StructField("site_id", StringType(), False)
])

print("IoT schema created successfully")

# COMMAND ----------

base_time = datetime.now()

iot_events = []

devices = [
    ("DEV-001", "temperature", "SITE-A"),
    ("DEV-002", "temperature", "SITE-A"),
    ("DEV-003", "vibration", "SITE-B"),
    ("DEV-004", "pressure", "SITE-B"),
    ("DEV-005", "multi_sensor", "SITE-C")
]

for i in range(50):
    device_id, sensor_type, site_id = random.choice(devices)

    temperature = round(random.uniform(25, 75), 2)
    vibration = round(random.uniform(0.2, 4.5), 2)
    pressure = round(random.uniform(90, 130), 2)

    # Inject a few anomaly records
    if i in [10, 22, 37]:
        temperature = round(random.uniform(90, 110), 2)
        vibration = round(random.uniform(7, 10), 2)
        pressure = round(random.uniform(145, 170), 2)

    event_time = base_time + timedelta(seconds=i * 10)

    iot_events.append(
        (
            f"EVT-{i+1:04d}",
            device_id,
            sensor_type,
            temperature,
            vibration,
            pressure,
            event_time,
            site_id
        )
    )

iot_df = spark.createDataFrame(iot_events, schema=iot_schema)

print("IoT events generated:", iot_df.count())

display(iot_df.orderBy("event_timestamp"))

# COMMAND ----------

source_table = "workspace.default.iot_source_events"

(
    iot_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(source_table)
)

print("IoT source Delta table created successfully")
print("Table:", source_table)
print("Rows :", spark.table(source_table).count())

# COMMAND ----------

anomaly_source_df = (
    spark.table(source_table)
    .filter(
        (F.col("temperature") >= 90) |
        (F.col("vibration") >= 7) |
        (F.col("pressure") >= 145)
    )
    .orderBy("event_timestamp")
)

print("Injected anomaly records:", anomaly_source_df.count())

display(anomaly_source_df)

# COMMAND ----------

device_summary = (
    spark.table(source_table)
    .groupBy("site_id", "device_id", "sensor_type")
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("temperature"), 2).alias("avg_temperature"),
        F.round(F.avg("vibration"), 2).alias("avg_vibration"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure")
    )
    .orderBy("site_id", "device_id")
)

display(device_summary)

# COMMAND ----------

source_dq_summary = spark.createDataFrame(
    [
        (
            "total_events",
            spark.table(source_table).count()
        ),
        (
            "duplicate_event_ids",
            spark.table(source_table)
            .groupBy("event_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        ),
        (
            "null_event_ids",
            spark.table(source_table)
            .filter(F.col("event_id").isNull())
            .count()
        ),
        (
            "null_device_ids",
            spark.table(source_table)
            .filter(F.col("device_id").isNull())
            .count()
        )
    ],
    ["check_name", "result"]
)

display(source_dq_summary)

# COMMAND ----------

print("========== IoT SOURCE GENERATION SUMMARY ==========")
print("Source Table        :", source_table)
print("Total Events        :", spark.table(source_table).count())
print("Distinct Devices    :", spark.table(source_table).select("device_id").distinct().count())
print("Distinct Sites      :", spark.table(source_table).select("site_id").distinct().count())
print("Injected Anomalies  :", anomaly_source_df.count())
print("Source Status       : READY")
print("===================================================")