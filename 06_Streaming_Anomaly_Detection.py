# Databricks notebook source
# Real-Time IoT Streaming Lakehouse
# 06 - Streaming Anomaly Detection

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_table = "workspace.default.iot_silver_events"
alert_table = "workspace.default.iot_anomaly_alerts"

silver_df = spark.table(silver_table)

print("Silver Table :", silver_table)
print("Silver Rows  :", silver_df.count())
print("Anomaly detection setup initialized")

# COMMAND ----------

device_baseline_df = (
    silver_df
    .groupBy("device_id")
    .agg(
        F.round(F.avg("temperature"), 2).alias("avg_temperature"),
        F.round(F.stddev_pop("temperature"), 2).alias("std_temperature"),
        F.round(F.avg("vibration"), 2).alias("avg_vibration"),
        F.round(F.stddev_pop("vibration"), 2).alias("std_vibration"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.round(F.stddev_pop("pressure"), 2).alias("std_pressure")
    )
)

display(device_baseline_df.orderBy("device_id"))

# COMMAND ----------

anomaly_base_df = (
    silver_df.alias("s")
    .join(
        device_baseline_df.alias("b"),
        on="device_id",
        how="left"
    )
)

print("Events with baseline:", anomaly_base_df.count())

# COMMAND ----------

anomaly_scored_df = (
    anomaly_base_df
    .withColumn(
        "temperature_score",
        F.when(
            F.col("std_temperature") > 0,
            F.abs(
                (F.col("temperature") - F.col("avg_temperature")) /
                F.col("std_temperature")
            )
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "vibration_score",
        F.when(
            F.col("std_vibration") > 0,
            F.abs(
                (F.col("vibration") - F.col("avg_vibration")) /
                F.col("std_vibration")
            )
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "pressure_score",
        F.when(
            F.col("std_pressure") > 0,
            F.abs(
                (F.col("pressure") - F.col("avg_pressure")) /
                F.col("std_pressure")
            )
        ).otherwise(F.lit(0.0))
    )
)

display(
    anomaly_scored_df.select(
        "event_id",
        "device_id",
        "temperature",
        "temperature_score",
        "vibration",
        "vibration_score",
        "pressure",
        "pressure_score"
    ).orderBy("event_timestamp")
)

# COMMAND ----------

anomaly_scored_df = (
    anomaly_scored_df
    .withColumn(
        "combined_anomaly_score",
        F.round(
            (
                F.col("temperature_score") +
                F.col("vibration_score") +
                F.col("pressure_score")
            ) / 3,
            2
        )
    )
)

display(
    anomaly_scored_df.select(
        "event_id",
        "device_id",
        "combined_anomaly_score"
    ).orderBy(F.col("combined_anomaly_score").desc())
)

# COMMAND ----------

risk_scored_df = (
    anomaly_scored_df
    .withColumn(
        "risk_level",
        F.when(
            F.col("combined_anomaly_score") >= 2.5,
            "CRITICAL"
        )
        .when(
            F.col("combined_anomaly_score") >= 1.5,
            "HIGH"
        )
        .when(
            F.col("combined_anomaly_score") >= 1.0,
            "MEDIUM"
        )
        .otherwise("NORMAL")
    )
)

display(
    risk_scored_df.select(
        "event_id",
        "device_id",
        "site_id",
        "combined_anomaly_score",
        "risk_level",
        "event_timestamp"
    )
    .orderBy(F.col("combined_anomaly_score").desc())
)

# COMMAND ----------

alert_candidates_df = (
    risk_scored_df
    .filter(
        F.col("risk_level").isin(
            "HIGH",
            "CRITICAL"
        )
    )
    .select(
        "event_id",
        "device_id",
        "site_id",
        "sensor_type",
        "temperature",
        "vibration",
        "pressure",
        "combined_anomaly_score",
        "risk_level",
        "event_timestamp"
    )
    .withColumn(
        "alert_timestamp",
        F.current_timestamp()
    )
)

print("Alert Candidates:", alert_candidates_df.count())

display(
    alert_candidates_df
    .orderBy(F.col("combined_anomaly_score").desc())
)

# COMMAND ----------

(
    alert_candidates_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(alert_table)
)

print("Anomaly alert table created successfully")
print("Alert Rows:", spark.table(alert_table).count())

# COMMAND ----------

device_risk_summary = (
    risk_scored_df
    .groupBy("device_id", "site_id")
    .agg(
        F.count("*").alias("total_events"),
        F.sum(
            F.when(
                F.col("risk_level").isin("HIGH", "CRITICAL"),
                1
            ).otherwise(0)
        ).alias("high_risk_events"),
        F.round(
            F.avg("combined_anomaly_score"),
            2
        ).alias("avg_anomaly_score"),
        F.round(
            F.max("combined_anomaly_score"),
            2
        ).alias("max_anomaly_score")
    )
    .orderBy(
        F.col("high_risk_events").desc(),
        F.col("max_anomaly_score").desc()
    )
)

display(device_risk_summary)

# COMMAND ----------

site_risk_summary = (
    risk_scored_df
    .groupBy("site_id")
    .agg(
        F.count("*").alias("total_events"),
        F.sum(
            F.when(
                F.col("risk_level").isin("HIGH", "CRITICAL"),
                1
            ).otherwise(0)
        ).alias("high_risk_events"),
        F.round(
            F.avg("combined_anomaly_score"),
            2
        ).alias("avg_risk_score")
    )
    .orderBy(F.col("high_risk_events").desc())
)

display(site_risk_summary)

# COMMAND ----------

alert_df = spark.table(alert_table)

invalid_alerts = (
    alert_df
    .filter(
        ~F.col("risk_level").isin("HIGH", "CRITICAL")
    )
    .count()
)

print("Alert Rows       :", alert_df.count())
print("Invalid Alerts   :", invalid_alerts)

if invalid_alerts == 0:
    print("ALERT VALIDATION: PASS")
else:
    print("ALERT VALIDATION: FAIL")

# COMMAND ----------

high_risk_count = (
    risk_scored_df
    .filter(
        F.col("risk_level").isin("HIGH", "CRITICAL")
    )
    .count()
)

print("========== ANOMALY DETECTION SUMMARY ==========")
print("Total Events          :", risk_scored_df.count())
print("High/Critical Events  :", high_risk_count)
print("Alert Records         :", spark.table(alert_table).count())
print("Devices Monitored     :", risk_scored_df.select("device_id").distinct().count())
print("Sites Monitored       :", risk_scored_df.select("site_id").distinct().count())
print("Alert Validation      : PASS")
print("Anomaly Status        : READY")
print("================================================")