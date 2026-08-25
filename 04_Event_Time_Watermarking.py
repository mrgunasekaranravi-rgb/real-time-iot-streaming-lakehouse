# Databricks notebook source
# Real-Time IoT Streaming Lakehouse
# 04 - Event Time Watermarking

from pyspark.sql import functions as F
from datetime import timedelta

silver_table = "workspace.default.iot_silver_events"

silver_df = spark.table(silver_table)

print("Silver Table :", silver_table)
print("Silver Rows  :", silver_df.count())
print("Event-time processing setup initialized")

# COMMAND ----------

event_time_stats = (
    silver_df
    .agg(
        F.min("event_timestamp").alias("earliest_event"),
        F.max("event_timestamp").alias("latest_event"),
        F.count("*").alias("total_events")
    )
)

display(event_time_stats)

# COMMAND ----------

latest_event_time = (
    silver_df
    .agg(F.max("event_timestamp").alias("latest"))
    .first()["latest"]
)

late_events = [
    (
        "EVT-LATE-001",
        "DEV-001",
        "temperature",
        72.50,
        1.50,
        111.20,
        latest_event_time - timedelta(minutes=20),
        "SITE-A"
    ),
    (
        "EVT-LATE-002",
        "DEV-003",
        "vibration",
        66.20,
        5.50,
        120.30,
        latest_event_time - timedelta(minutes=12),
        "SITE-B"
    ),
    (
        "EVT-ONTIME-001",
        "DEV-005",
        "multi_sensor",
        81.40,
        4.20,
        132.10,
        latest_event_time + timedelta(seconds=30),
        "SITE-C"
    )
]

late_event_schema = """
event_id STRING,
device_id STRING,
sensor_type STRING,
temperature DOUBLE,
vibration DOUBLE,
pressure DOUBLE,
event_timestamp TIMESTAMP,
site_id STRING
"""

late_events_df = spark.createDataFrame(
    late_events,
    late_event_schema
)

display(
    late_events_df.orderBy("event_timestamp")
)

# COMMAND ----------

watermark_threshold_minutes = 10

classified_events_df = (
    late_events_df
    .withColumn(
        "minutes_behind_latest",
        F.round(
            (
                F.unix_timestamp(F.lit(latest_event_time)) -
                F.unix_timestamp("event_timestamp")
            ) / 60,
            2
        )
    )
    .withColumn(
        "arrival_classification",
        F.when(
            F.col("event_timestamp")
            < F.lit(latest_event_time) -
              F.expr(
                  f"INTERVAL {watermark_threshold_minutes} MINUTES"
              ),
            "LATE_BEYOND_WATERMARK"
        )
        .otherwise("WITHIN_WATERMARK")
    )
)

display(
    classified_events_df.select(
        "event_id",
        "device_id",
        "event_timestamp",
        "minutes_behind_latest",
        "arrival_classification"
    ).orderBy("event_timestamp")
)

# COMMAND ----------

watermarked_stream_df = (
    spark.readStream
    .format("delta")
    .table(silver_table)
    .withWatermark(
        "event_timestamp",
        "10 minutes"
    )
)

print("Streaming DataFrame :", watermarked_stream_df.isStreaming)
print("Watermark Column    : event_timestamp")
print("Watermark Threshold : 10 minutes")

# COMMAND ----------

deduplicated_stream_df = (
    watermarked_stream_df
    .dropDuplicatesWithinWatermark(["event_id"])
)

print(
    "Watermark-aware deduplication configured successfully"
)
print(
    "Is Streaming:",
    deduplicated_stream_df.isStreaming
)

# COMMAND ----------

spark.sql("""
CREATE VOLUME IF NOT EXISTS
workspace.default.iot_streaming_checkpoints
""")

watermark_checkpoint = (
    "/Volumes/workspace/default/"
    "iot_streaming_checkpoints/watermark_dedup"
)

watermark_table = (
    "workspace.default.iot_watermarked_events"
)

print("Watermark Target     :", watermark_table)
print("Watermark Checkpoint :", watermark_checkpoint)

# COMMAND ----------

watermark_query = (
    deduplicated_stream_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        watermark_checkpoint
    )
    .trigger(availableNow=True)
    .toTable(watermark_table)
)

watermark_query.awaitTermination()

print(
    "Watermark streaming pipeline completed successfully"
)

# COMMAND ----------

watermarked_output_df = spark.table(watermark_table)

print(
    "Watermarked Output Rows:",
    watermarked_output_df.count()
)

display(
    watermarked_output_df
    .select(
        "event_id",
        "device_id",
        "event_timestamp",
        "is_anomaly",
        "anomaly_severity"
    )
    .orderBy("event_timestamp")
)

# COMMAND ----------

duplicate_count = (
    watermarked_output_df
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Duplicate Event IDs:",
    duplicate_count
)

if duplicate_count == 0:
    print("WATERMARK DEDUPLICATION CHECK: PASS")
else:
    print("WATERMARK DEDUPLICATION CHECK: FAIL")

# COMMAND ----------

late_count = (
    classified_events_df
    .filter(
        F.col("arrival_classification")
        == "LATE_BEYOND_WATERMARK"
    )
    .count()
)

within_watermark_count = (
    classified_events_df
    .filter(
        F.col("arrival_classification")
        == "WITHIN_WATERMARK"
    )
    .count()
)

print("Late Beyond Watermark :", late_count)
print("Within Watermark      :", within_watermark_count)

# COMMAND ----------

print("========== EVENT-TIME PROCESSING SUMMARY ==========")
print("Silver Events             :", silver_df.count())
print("Watermark Threshold       : 10 minutes")
print("Late Events Simulated     :", 2)
print("Within Watermark Events   :", 1)
print("Duplicate Event IDs       :", duplicate_count)
print("Structured Streaming      :", True)
print("Watermark Enabled         :", True)
print("Event-Time Status         : READY")
print("===================================================")