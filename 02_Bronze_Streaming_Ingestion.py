# Databricks notebook source
# Real-Time IoT Streaming Lakehouse
# 02 - Bronze Streaming Ingestion

from pyspark.sql import functions as F
from datetime import datetime, timedelta

source_table = "workspace.default.iot_source_events"
bronze_table = "workspace.default.iot_bronze_events"

source_df = spark.table(source_table)

print("Source table :", source_table)
print("Source rows  :", source_df.count())
print("Bronze streaming setup initialized")

# COMMAND ----------

bronze_stream_df = (
    spark.readStream
    .format("delta")
    .table(source_table)
    .withColumn("bronze_ingestion_timestamp", F.current_timestamp())
    .withColumn("bronze_ingestion_date", F.current_date())
    .withColumn("source_system", F.lit("IOT_SENSOR_PLATFORM"))
)

print("Streaming DataFrame created successfully")
print("Is Streaming:", bronze_stream_df.isStreaming)

# COMMAND ----------

spark.sql("""
CREATE VOLUME IF NOT EXISTS workspace.default.iot_streaming_checkpoints
""")

checkpoint_path = (
    "/Volumes/workspace/default/"
    "iot_streaming_checkpoints/bronze_ingestion"
)

print("Checkpoint storage ready")
print("Checkpoint Path:", checkpoint_path)

# COMMAND ----------

bronze_query = (
    bronze_stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(bronze_table)
)

bronze_query.awaitTermination()

print("Bronze streaming ingestion completed successfully")

# COMMAND ----------

bronze_df = spark.table(bronze_table)

print("Bronze Table :", bronze_table)
print("Bronze Rows  :", bronze_df.count())

display(
    bronze_df
    .select(
        "event_id",
        "device_id",
        "sensor_type",
        "temperature",
        "vibration",
        "pressure",
        "event_timestamp",
        "site_id",
        "bronze_ingestion_timestamp",
        "source_system"
    )
    .orderBy("event_timestamp")
)

# COMMAND ----------

current_max_time = (
    spark.table(source_table)
    .agg(F.max("event_timestamp").alias("max_time"))
    .first()["max_time"]
)

new_events = [
    (
        "EVT-0051",
        "DEV-001",
        "temperature",
        68.50,
        1.20,
        110.40,
        current_max_time + timedelta(seconds=10),
        "SITE-A"
    ),
    (
        "EVT-0052",
        "DEV-003",
        "vibration",
        55.20,
        3.80,
        118.10,
        current_max_time + timedelta(seconds=20),
        "SITE-B"
    ),
    (
        "EVT-0053",
        "DEV-004",
        "pressure",
        62.40,
        2.10,
        128.50,
        current_max_time + timedelta(seconds=30),
        "SITE-B"
    ),
    (
        "EVT-0054",
        "DEV-005",
        "multi_sensor",
        96.50,
        8.20,
        154.30,
        current_max_time + timedelta(seconds=40),
        "SITE-C"
    ),
    (
        "EVT-0055",
        "DEV-002",
        "temperature",
        47.80,
        1.40,
        105.70,
        current_max_time + timedelta(seconds=50),
        "SITE-A"
    )
]

new_events_df = spark.createDataFrame(
    new_events,
    schema=spark.table(source_table).schema
)

display(new_events_df)

# COMMAND ----------

(
    new_events_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(source_table)
)

print("New IoT events appended successfully")
print("Current Source Rows:", spark.table(source_table).count())

# COMMAND ----------

incremental_query = (
    spark.readStream
    .format("delta")
    .table(source_table)
    .withColumn("bronze_ingestion_timestamp", F.current_timestamp())
    .withColumn("bronze_ingestion_date", F.current_date())
    .withColumn("source_system", F.lit("IOT_SENSOR_PLATFORM"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(bronze_table)
)

incremental_query.awaitTermination()

print("Incremental Bronze streaming ingestion completed successfully")

# COMMAND ----------

final_bronze_df = spark.table(bronze_table)

source_count = spark.table(source_table).count()
bronze_count = final_bronze_df.count()

print("Source Rows :", source_count)
print("Bronze Rows :", bronze_count)

if source_count == 55 and bronze_count == 55:
    print("INCREMENTAL STREAMING CHECK: PASS")
else:
    print("INCREMENTAL STREAMING CHECK: FAIL")

display(
    final_bronze_df
    .filter(F.col("event_id").isin(
        "EVT-0051",
        "EVT-0052",
        "EVT-0053",
        "EVT-0054",
        "EVT-0055"
    ))
    .orderBy("event_timestamp")
)

# COMMAND ----------

bronze_validation = spark.createDataFrame(
    [
        (
            "total_bronze_events",
            final_bronze_df.count()
        ),
        (
            "duplicate_event_ids",
            final_bronze_df
            .groupBy("event_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        ),
        (
            "null_event_ids",
            final_bronze_df
            .filter(F.col("event_id").isNull())
            .count()
        ),
        (
            "null_device_ids",
            final_bronze_df
            .filter(F.col("device_id").isNull())
            .count()
        )
    ],
    ["validation_check", "result"]
)

display(bronze_validation)

# COMMAND ----------

print("========== BRONZE STREAMING SUMMARY ==========")
print("Source Table        :", source_table)
print("Bronze Table        :", bronze_table)
print("Streaming Enabled   :", True)
print("Checkpoint Enabled  :", True)
print("Source Events       :", spark.table(source_table).count())
print("Bronze Events       :", spark.table(bronze_table).count())
print("Incremental Events  :", 5)
print("Bronze Status       : READY")
print("==============================================")