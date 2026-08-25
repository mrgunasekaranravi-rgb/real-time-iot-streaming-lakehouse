# Databricks notebook source
# Real-Time IoT Streaming Lakehouse
# 05 - Streaming Window Aggregations

from pyspark.sql import functions as F

silver_table = "workspace.default.iot_silver_events"
window_table = "workspace.default.iot_window_metrics"

print("Silver Table :", silver_table)
print("Silver Rows  :", spark.table(silver_table).count())
print("Window aggregation setup initialized")

# COMMAND ----------

stream_df = (
    spark.readStream
    .format("delta")
    .table(silver_table)
    .withWatermark("event_timestamp", "10 minutes")
)

print("Streaming Source:", stream_df.isStreaming)

# COMMAND ----------

window_agg_df = (
    stream_df
    .groupBy(
        F.window("event_timestamp", "5 minutes"),
        "site_id",
        "device_id"
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("temperature"), 2).alias("avg_temperature"),
        F.round(F.max("temperature"), 2).alias("max_temperature"),
        F.round(F.avg("vibration"), 2).alias("avg_vibration"),
        F.round(F.max("vibration"), 2).alias("max_vibration"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.round(F.max("pressure"), 2).alias("max_pressure"),
        F.sum(
            F.when(F.col("is_anomaly") == True, 1).otherwise(0)
        ).alias("anomaly_count")
    )
)

print("Window aggregation configured")
print("Is Streaming:", window_agg_df.isStreaming)

# COMMAND ----------

spark.sql("""
CREATE VOLUME IF NOT EXISTS
workspace.default.iot_streaming_checkpoints
""")

window_checkpoint = (
    "/Volumes/workspace/default/"
    "iot_streaming_checkpoints/window_aggregation"
)

print("Checkpoint:", window_checkpoint)

# COMMAND ----------

# Rebuild window metrics using COMPLETE mode for the bounded demo

spark.sql(f"DROP TABLE IF EXISTS {window_table}")

window_checkpoint_v2 = (
    "/Volumes/workspace/default/"
    "iot_streaming_checkpoints/window_aggregation_v2"
)

window_query = (
    window_agg_df
    .writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", window_checkpoint_v2)
    .trigger(availableNow=True)
    .toTable(window_table)
)

window_query.awaitTermination()

print("Streaming window aggregation completed successfully")
print("Output Mode: COMPLETE")

# COMMAND ----------

window_output_df = spark.table(window_table)

print("Window Metric Rows:", window_output_df.count())

display(
    window_output_df
    .select(
        "window",
        "site_id",
        "device_id",
        "event_count",
        "avg_temperature",
        "max_temperature",
        "avg_vibration",
        "max_vibration",
        "avg_pressure",
        "max_pressure",
        "anomaly_count"
    )
    .orderBy("window.start", "site_id", "device_id")
)

# COMMAND ----------

site_window_summary = (
    window_output_df
    .groupBy("site_id")
    .agg(
        F.sum("event_count").alias("total_events"),
        F.sum("anomaly_count").alias("total_anomalies"),
        F.round(F.avg("avg_temperature"), 2).alias("overall_avg_temperature"),
        F.round(F.avg("avg_vibration"), 2).alias("overall_avg_vibration"),
        F.round(F.avg("avg_pressure"), 2).alias("overall_avg_pressure")
    )
    .orderBy(F.col("total_anomalies").desc(), "site_id")
)

display(site_window_summary)

# COMMAND ----------

high_risk_windows = (
    window_output_df
    .filter(F.col("anomaly_count") > 0)
    .orderBy(
        F.col("anomaly_count").desc(),
        F.col("max_temperature").desc(),
        F.col("max_vibration").desc()
    )
)

print("High-Risk Windows:", high_risk_windows.count())

display(high_risk_windows)

# COMMAND ----------

window_event_total = (
    window_output_df
    .agg(F.sum("event_count").alias("total"))
    .first()["total"]
)

silver_count = spark.table(silver_table).count()

print("Silver Event Count      :", silver_count)
print("Window Aggregated Events:", window_event_total)

if window_event_total == silver_count:
    print("WINDOW AGGREGATION RECONCILIATION: PASS")
else:
    print("WINDOW AGGREGATION RECONCILIATION: CHECK REQUIRED")

# COMMAND ----------

print("========== STREAMING WINDOW SUMMARY ==========")
print("Window Type          : Tumbling")
print("Window Duration      : 5 minutes")
print("Watermark            : 10 minutes")
print("Source Events        :", silver_count)
print("Window Metric Rows   :", window_output_df.count())
print("High-Risk Windows    :", high_risk_windows.count())
print("Structured Streaming : True")
print("Window Status        : READY")
print("==============================================")