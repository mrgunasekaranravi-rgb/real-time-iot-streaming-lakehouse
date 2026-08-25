# Databricks notebook source
# Real-Time IoT Streaming Lakehouse
# 08 - Streaming Monitoring and Audit

from pyspark.sql import functions as F
from datetime import datetime
import uuid

bronze_table = "workspace.default.iot_bronze_events"
silver_table = "workspace.default.iot_silver_events"
alert_table = "workspace.default.iot_anomaly_alerts"

audit_table = "workspace.default.iot_pipeline_audit"

print("Monitoring & Audit setup initialized")
print("Audit Table:", audit_table)

# COMMAND ----------

run_id = str(uuid.uuid4())

pipeline_name = "Real_Time_IoT_Streaming_Lakehouse"
run_start_time = datetime.now()

print("Run ID        :", run_id)
print("Pipeline Name :", pipeline_name)
print("Start Time    :", run_start_time)
print("Run Status    : STARTED")

# COMMAND ----------

bronze_count = spark.table(bronze_table).count()
silver_count = spark.table(silver_table).count()
alert_count = spark.table(alert_table).count()

anomaly_count = (
    spark.table(silver_table)
    .filter(F.col("is_anomaly") == True)
    .count()
)

device_count = (
    spark.table(silver_table)
    .select("device_id")
    .distinct()
    .count()
)

site_count = (
    spark.table(silver_table)
    .select("site_id")
    .distinct()
    .count()
)

print("Bronze Rows    :", bronze_count)
print("Silver Rows    :", silver_count)
print("Anomaly Events :", anomaly_count)
print("Alert Records  :", alert_count)
print("Devices        :", device_count)
print("Sites          :", site_count)

# COMMAND ----------

reconciliation_status = (
    "PASS"
    if bronze_count == silver_count
    else "FAIL"
)

duplicate_count = (
    spark.table(silver_table)
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_key_count = (
    spark.table(silver_table)
    .filter(
        F.col("event_id").isNull() |
        F.col("device_id").isNull()
    )
    .count()
)

pipeline_status = (
    "SUCCESS"
    if (
        reconciliation_status == "PASS"
        and duplicate_count == 0
        and null_key_count == 0
    )
    else "FAILED"
)

print("Reconciliation :", reconciliation_status)
print("Duplicates     :", duplicate_count)
print("Null Keys      :", null_key_count)
print("Pipeline Status:", pipeline_status)

# COMMAND ----------

run_end_time = datetime.now()

duration_seconds = round(
    (run_end_time - run_start_time).total_seconds(),
    3
)

print("Start Time       :", run_start_time)
print("End Time         :", run_end_time)
print("Duration Seconds :", duration_seconds)
print("Status           :", pipeline_status)

# COMMAND ----------

audit_record = [
    (
        run_id,
        pipeline_name,
        run_start_time,
        run_end_time,
        float(duration_seconds),
        int(bronze_count),
        int(silver_count),
        int(anomaly_count),
        int(alert_count),
        int(device_count),
        int(site_count),
        int(duplicate_count),
        int(null_key_count),
        reconciliation_status,
        pipeline_status,
        None
    )
]

audit_schema = """
run_id STRING,
pipeline_name STRING,
start_time TIMESTAMP,
end_time TIMESTAMP,
duration_seconds DOUBLE,
bronze_rows LONG,
silver_rows LONG,
anomaly_events LONG,
alert_records LONG,
device_count LONG,
site_count LONG,
duplicate_events LONG,
null_key_events LONG,
reconciliation_status STRING,
status STRING,
error_message STRING
"""

audit_df = spark.createDataFrame(
    audit_record,
    schema=audit_schema
)

display(audit_df)

# COMMAND ----------

(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(audit_table)
)

print("Pipeline audit record saved successfully")
print(
    "Total Audit Records:",
    spark.table(audit_table).count()
)

# COMMAND ----------

audit_history_df = (
    spark.table(audit_table)
    .orderBy(F.col("start_time").desc())
)

display(
    audit_history_df.select(
        "run_id",
        "pipeline_name",
        "start_time",
        "end_time",
        "duration_seconds",
        "bronze_rows",
        "silver_rows",
        "anomaly_events",
        "alert_records",
        "reconciliation_status",
        "status",
        "error_message"
    )
)

# COMMAND ----------

tables_to_monitor = [
    ("Bronze", "workspace.default.iot_bronze_events"),
    ("Silver", "workspace.default.iot_silver_events"),
    ("Alerts", "workspace.default.iot_anomaly_alerts"),
    ("Gold Device", "workspace.default.iot_gold_device_health"),
    ("Gold Site", "workspace.default.iot_gold_site_health"),
    ("Gold Alerts", "workspace.default.iot_gold_alert_summary"),
    ("Gold KPIs", "workspace.default.iot_gold_operational_kpis")
]

table_health = []

for layer, table_name in tables_to_monitor:
    try:
        row_count = spark.table(table_name).count()
        table_health.append(
            (layer, table_name, True, row_count, "HEALTHY")
        )
    except Exception:
        table_health.append(
            (layer, table_name, False, 0, "UNAVAILABLE")
        )

table_health_df = spark.createDataFrame(
    table_health,
    [
        "layer",
        "table_name",
        "table_exists",
        "row_count",
        "health_status"
    ]
)

display(table_health_df)

# COMMAND ----------

monitoring_summary_df = (
    spark.table(audit_table)
    .groupBy("status")
    .agg(
        F.count("*").alias("run_count"),
        F.sum("silver_rows").alias("total_rows_processed"),
        F.round(
            F.avg("duration_seconds"),
            3
        ).alias("avg_duration_seconds"),
        F.max("start_time").alias("latest_run_time")
    )
)

display(monitoring_summary_df)

# COMMAND ----------

unhealthy_tables = (
    table_health_df
    .filter(F.col("health_status") != "HEALTHY")
    .count()
)

failed_runs = (
    spark.table(audit_table)
    .filter(F.col("status") == "FAILED")
    .count()
)

overall_health = (
    "HEALTHY"
    if unhealthy_tables == 0 and pipeline_status == "SUCCESS"
    else "ATTENTION_REQUIRED"
)

print("Unhealthy Tables :", unhealthy_tables)
print("Historical Failed Runs :", failed_runs)
print("Current Run Status:", pipeline_status)
print("Overall Health    :", overall_health)

# COMMAND ----------

print("========== STREAMING MONITORING SUMMARY ==========")
print("Pipeline Name        :", pipeline_name)
print("Current Run ID       :", run_id)
print("Bronze Rows          :", bronze_count)
print("Silver Rows          :", silver_count)
print("Anomaly Events       :", anomaly_count)
print("Alert Records        :", alert_count)
print("Reconciliation       :", reconciliation_status)
print("Current Run Status   :", pipeline_status)
print("Tables Monitored     :", len(tables_to_monitor))
print("Unhealthy Tables     :", unhealthy_tables)
print("Overall Health       :", overall_health)
print("Monitoring Status    : READY")
print("==================================================")