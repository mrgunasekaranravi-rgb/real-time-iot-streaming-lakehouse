# Databricks notebook source
# Real-Time IoT Streaming Lakehouse
# 03 - Silver Streaming Transformation

from pyspark.sql import functions as F

bronze_table = "workspace.default.iot_bronze_events"
silver_table = "workspace.default.iot_silver_events"

bronze_df = spark.table(bronze_table)

print("Bronze Table :", bronze_table)
print("Bronze Rows  :", bronze_df.count())
print("Silver transformation setup initialized")

# COMMAND ----------

silver_base_df = (
    bronze_df
    .dropDuplicates(["event_id"])
    .filter(
        F.col("event_id").isNotNull() &
        F.col("device_id").isNotNull() &
        F.col("event_timestamp").isNotNull() &
        F.col("site_id").isNotNull()
    )
    .withColumn("device_id", F.upper(F.trim("device_id")))
    .withColumn("sensor_type", F.lower(F.trim("sensor_type")))
    .withColumn("site_id", F.upper(F.trim("site_id")))
    .withColumn(
        "temperature",
        F.round(F.col("temperature"), 2)
    )
    .withColumn(
        "vibration",
        F.round(F.col("vibration"), 2)
    )
    .withColumn(
        "pressure",
        F.round(F.col("pressure"), 2)
    )
)

display(silver_base_df.orderBy("event_timestamp"))

# COMMAND ----------

silver_enriched_df = (
    silver_base_df
    .withColumn(
        "is_anomaly",
        (
            (F.col("temperature") >= 90) |
            (F.col("vibration") >= 7) |
            (F.col("pressure") >= 145)
        )
    )
    .withColumn(
        "anomaly_severity",
        F.when(
            (
                (F.col("temperature") >= 100) |
                (F.col("vibration") >= 9) |
                (F.col("pressure") >= 160)
            ),
            F.lit("CRITICAL")
        )
        .when(
            (
                (F.col("temperature") >= 90) |
                (F.col("vibration") >= 7) |
                (F.col("pressure") >= 145)
            ),
            F.lit("HIGH")
        )
        .otherwise(F.lit("NORMAL"))
    )
)

display(
    silver_enriched_df.select(
        "event_id",
        "device_id",
        "temperature",
        "vibration",
        "pressure",
        "is_anomaly",
        "anomaly_severity",
        "event_timestamp"
    ).orderBy("event_timestamp")
)

# COMMAND ----------

silver_final_df = (
    silver_enriched_df
    .withColumn(
        "temperature_status",
        F.when(F.col("temperature") >= 90, "HIGH")
         .when(F.col("temperature") < 20, "LOW")
         .otherwise("NORMAL")
    )
    .withColumn(
        "vibration_status",
        F.when(F.col("vibration") >= 7, "HIGH")
         .otherwise("NORMAL")
    )
    .withColumn(
        "pressure_status",
        F.when(F.col("pressure") >= 145, "HIGH")
         .otherwise("NORMAL")
    )
    .withColumn(
        "event_date",
        F.to_date("event_timestamp")
    )
    .withColumn(
        "event_hour",
        F.hour("event_timestamp")
    )
)

display(silver_final_df.orderBy("event_timestamp"))

# COMMAND ----------

(
    silver_final_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

print("Silver Delta table created successfully")
print("Silver Rows:", spark.table(silver_table).count())

# COMMAND ----------

silver_check_df = spark.table(silver_table)

duplicate_events = (
    silver_check_df
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_event_ids = (
    silver_check_df
    .filter(F.col("event_id").isNull())
    .count()
)

null_device_ids = (
    silver_check_df
    .filter(F.col("device_id").isNull())
    .count()
)

print("Silver Rows       :", silver_check_df.count())
print("Duplicate Events  :", duplicate_events)
print("Null Event IDs    :", null_event_ids)
print("Null Device IDs   :", null_device_ids)

# COMMAND ----------

anomaly_summary_df = (
    silver_check_df
    .groupBy("anomaly_severity")
    .agg(
        F.count("*").alias("event_count")
    )
    .orderBy("anomaly_severity")
)

display(anomaly_summary_df)

# COMMAND ----------

device_anomaly_summary = (
    silver_check_df
    .groupBy("device_id", "site_id")
    .agg(
        F.count("*").alias("total_events"),
        F.sum(
            F.when(F.col("is_anomaly") == True, 1).otherwise(0)
        ).alias("anomaly_events"),
        F.round(F.avg("temperature"), 2).alias("avg_temperature"),
        F.round(F.avg("vibration"), 2).alias("avg_vibration"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure")
    )
    .orderBy(F.col("anomaly_events").desc(), "device_id")
)

display(device_anomaly_summary)

# COMMAND ----------

bronze_count = spark.table(bronze_table).count()
silver_count = spark.table(silver_table).count()

print("Bronze Rows :", bronze_count)
print("Silver Rows :", silver_count)

if bronze_count == silver_count:
    print("BRONZE -> SILVER RECONCILIATION: PASS")
else:
    print("BRONZE -> SILVER RECONCILIATION: FAIL")

# COMMAND ----------

anomaly_count = (
    spark.table(silver_table)
    .filter(F.col("is_anomaly") == True)
    .count()
)

print("========== SILVER TRANSFORMATION SUMMARY ==========")
print("Bronze Events       :", spark.table(bronze_table).count())
print("Silver Events       :", spark.table(silver_table).count())
print("Duplicate Events    :", duplicate_events)
print("Detected Anomalies  :", anomaly_count)
print("Silver Status       : READY")
print("===================================================")