# Databricks notebook source
# Real-Time IoT Streaming Lakehouse
# 07 - Gold Streaming Metrics

from pyspark.sql import functions as F

silver_table = "workspace.default.iot_silver_events"
alert_table = "workspace.default.iot_anomaly_alerts"

gold_device_table = "workspace.default.iot_gold_device_health"
gold_site_table = "workspace.default.iot_gold_site_health"
gold_alert_table = "workspace.default.iot_gold_alert_summary"
gold_kpi_table = "workspace.default.iot_gold_operational_kpis"

silver_df = spark.table(silver_table)
alert_df = spark.table(alert_table)

print("Silver Rows :", silver_df.count())
print("Alert Rows  :", alert_df.count())
print("Gold metrics setup initialized")

# COMMAND ----------

device_health_df = (
    silver_df
    .groupBy("device_id", "site_id")
    .agg(
        F.count("*").alias("total_events"),
        F.sum(
            F.when(F.col("is_anomaly") == True, 1).otherwise(0)
        ).alias("anomaly_events"),
        F.round(F.avg("temperature"), 2).alias("avg_temperature"),
        F.round(F.max("temperature"), 2).alias("max_temperature"),
        F.round(F.avg("vibration"), 2).alias("avg_vibration"),
        F.round(F.max("vibration"), 2).alias("max_vibration"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.round(F.max("pressure"), 2).alias("max_pressure")
    )
    .withColumn(
        "anomaly_rate_pct",
        F.round(
            (F.col("anomaly_events") / F.col("total_events")) * 100,
            2
        )
    )
    .withColumn(
        "device_health_status",
        F.when(F.col("anomaly_rate_pct") >= 20, "CRITICAL")
         .when(F.col("anomaly_rate_pct") >= 10, "WARNING")
         .otherwise("HEALTHY")
    )
    .orderBy(F.col("anomaly_rate_pct").desc())
)

display(device_health_df)

# COMMAND ----------

(
    device_health_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_device_table)
)

print("Gold device health table created")
print("Rows:", spark.table(gold_device_table).count())

# COMMAND ----------

site_health_df = (
    silver_df
    .groupBy("site_id")
    .agg(
        F.count("*").alias("total_events"),
        F.countDistinct("device_id").alias("device_count"),
        F.sum(
            F.when(F.col("is_anomaly") == True, 1).otherwise(0)
        ).alias("anomaly_events"),
        F.round(F.avg("temperature"), 2).alias("avg_temperature"),
        F.round(F.avg("vibration"), 2).alias("avg_vibration"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure")
    )
    .withColumn(
        "anomaly_rate_pct",
        F.round(
            (F.col("anomaly_events") / F.col("total_events")) * 100,
            2
        )
    )
    .withColumn(
        "site_health_status",
        F.when(F.col("anomaly_rate_pct") >= 20, "CRITICAL")
         .when(F.col("anomaly_rate_pct") >= 10, "WARNING")
         .otherwise("HEALTHY")
    )
    .orderBy(F.col("anomaly_rate_pct").desc())
)

display(site_health_df)

# COMMAND ----------

(
    site_health_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_site_table)
)

print("Gold site health table created")
print("Rows:", spark.table(gold_site_table).count())

# COMMAND ----------

alert_summary_df = (
    alert_df
    .groupBy("site_id", "risk_level")
    .agg(
        F.count("*").alias("alert_count"),
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
        "site_id",
        F.col("alert_count").desc()
    )
)

display(alert_summary_df)

# COMMAND ----------

(
    alert_summary_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_alert_table)
)

print("Gold alert summary table created")
print("Rows:", spark.table(gold_alert_table).count())

# COMMAND ----------

total_events = silver_df.count()

total_anomalies = (
    silver_df
    .filter(F.col("is_anomaly") == True)
    .count()
)

total_alerts = alert_df.count()

total_devices = (
    silver_df.select("device_id").distinct().count()
)

total_sites = (
    silver_df.select("site_id").distinct().count()
)

anomaly_rate = round(
    (total_anomalies / total_events) * 100,
    2
) if total_events > 0 else 0.0

operational_kpis = [
    ("total_events", float(total_events)),
    ("total_anomalies", float(total_anomalies)),
    ("total_alerts", float(total_alerts)),
    ("total_devices", float(total_devices)),
    ("total_sites", float(total_sites)),
    ("anomaly_rate_pct", float(anomaly_rate))
]

operational_kpi_df = spark.createDataFrame(
    operational_kpis,
    ["kpi_name", "kpi_value"]
)

display(operational_kpi_df)

# COMMAND ----------

(
    operational_kpi_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_kpi_table)
)

print("Gold operational KPI table created successfully")

# COMMAND ----------

top_risk_devices = (
    spark.table(gold_device_table)
    .orderBy(
        F.col("anomaly_rate_pct").desc(),
        F.col("anomaly_events").desc()
    )
)

display(top_risk_devices)

# COMMAND ----------

gold_tables = [
    gold_device_table,
    gold_site_table,
    gold_alert_table,
    gold_kpi_table
]

gold_validation_results = []

for table_name in gold_tables:
    df = spark.table(table_name)

    gold_validation_results.append(
        (
            table_name,
            True,
            df.count()
        )
    )

gold_validation_df = spark.createDataFrame(
    gold_validation_results,
    ["table_name", "exists", "row_count"]
)

display(gold_validation_df)

# COMMAND ----------

print("========== GOLD STREAMING METRICS SUMMARY ==========")
print("Device Health Rows   :", spark.table(gold_device_table).count())
print("Site Health Rows     :", spark.table(gold_site_table).count())
print("Alert Summary Rows   :", spark.table(gold_alert_table).count())
print("Operational KPI Rows :", spark.table(gold_kpi_table).count())
print("Gold Status          : READY")
print("====================================================")