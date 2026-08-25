# Databricks notebook source
# Real-Time IoT Streaming Lakehouse
# 11 - Project Validation

from pyspark.sql import functions as F

project_name = "Real-Time IoT Streaming Lakehouse"

source_table = "workspace.default.iot_source_events"
bronze_table = "workspace.default.iot_bronze_events"
silver_table = "workspace.default.iot_silver_events"
watermark_table = "workspace.default.iot_watermarked_events"
window_table = "workspace.default.iot_window_metrics"
alert_table = "workspace.default.iot_anomaly_alerts"

gold_device_table = "workspace.default.iot_gold_device_health"
gold_site_table = "workspace.default.iot_gold_site_health"
gold_alert_table = "workspace.default.iot_gold_alert_summary"
gold_kpi_table = "workspace.default.iot_gold_operational_kpis"

audit_table = "workspace.default.iot_pipeline_audit"

print("Project :", project_name)
print("Final validation initialized")

# COMMAND ----------

project_tables = [
    ("Source", source_table),
    ("Bronze", bronze_table),
    ("Silver", silver_table),
    ("Watermark", watermark_table),
    ("Window Metrics", window_table),
    ("Anomaly Alerts", alert_table),
    ("Gold Device Health", gold_device_table),
    ("Gold Site Health", gold_site_table),
    ("Gold Alert Summary", gold_alert_table),
    ("Gold Operational KPIs", gold_kpi_table),
    ("Pipeline Audit", audit_table)
]

validation_rows = []

for layer, table_name in project_tables:
    try:
        row_count = spark.table(table_name).count()

        validation_rows.append(
            (layer, table_name, True, row_count, "PASS")
        )

    except Exception:
        validation_rows.append(
            (layer, table_name, False, 0, "FAIL")
        )

table_validation_df = spark.createDataFrame(
    validation_rows,
    [
        "layer",
        "table_name",
        "table_exists",
        "row_count",
        "validation_status"
    ]
)

display(table_validation_df)

# COMMAND ----------

source_count = spark.table(source_table).count()
bronze_count = spark.table(bronze_table).count()
silver_count = spark.table(silver_table).count()

reconciliation_status = (
    "PASS"
    if source_count == bronze_count == silver_count
    else "FAIL"
)

reconciliation_df = spark.createDataFrame(
    [
        ("Source", source_count),
        ("Bronze", bronze_count),
        ("Silver", silver_count)
    ],
    ["layer", "row_count"]
)

display(reconciliation_df)

print("Reconciliation Status:", reconciliation_status)

# COMMAND ----------

silver_df = spark.table(silver_table)

duplicate_events = (
    silver_df
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_event_ids = (
    silver_df
    .filter(F.col("event_id").isNull())
    .count()
)

null_device_ids = (
    silver_df
    .filter(F.col("device_id").isNull())
    .count()
)

dq_status = (
    "PASS"
    if (
        duplicate_events == 0
        and null_event_ids == 0
        and null_device_ids == 0
    )
    else "FAIL"
)

print("Duplicate Events :", duplicate_events)
print("Null Event IDs   :", null_event_ids)
print("Null Device IDs  :", null_device_ids)
print("Data Quality     :", dq_status)

# COMMAND ----------

watermark_count = spark.table(watermark_table).count()
window_count = spark.table(window_table).count()
alert_count = spark.table(alert_table).count()

streaming_validation_df = spark.createDataFrame(
    [
        ("Watermarked Events", watermark_count, "PASS" if watermark_count > 0 else "FAIL"),
        ("Window Metrics", window_count, "PASS" if window_count > 0 else "FAIL"),
        ("Anomaly Alerts", alert_count, "PASS" if alert_count > 0 else "FAIL")
    ],
    ["streaming_component", "row_count", "status"]
)

display(streaming_validation_df)

# COMMAND ----------

print("Watermark Rows :", spark.table("workspace.default.iot_watermarked_events").count())
print("Window Rows    :", spark.table("workspace.default.iot_window_metrics").count())
print("Alert Rows     :", spark.table("workspace.default.iot_anomaly_alerts").count())

# COMMAND ----------

device_count = (
    silver_df
    .select("device_id")
    .distinct()
    .count()
)

site_count = (
    silver_df
    .select("site_id")
    .distinct()
    .count()
)

print("Devices Monitored :", device_count)
print("Sites Monitored   :", site_count)

coverage_status = (
    "PASS"
    if device_count == 5 and site_count == 3
    else "CHECK"
)

print("Coverage Status   :", coverage_status)

# COMMAND ----------

gold_validation = []

for table_name in [
    gold_device_table,
    gold_site_table,
    gold_alert_table,
    gold_kpi_table
]:
    try:
        row_count = spark.table(table_name).count()

        gold_validation.append(
            (
                table_name,
                row_count,
                "PASS"
            )
        )

    except Exception:
        gold_validation.append(
            (
                table_name,
                0,
                "FAIL"
            )
        )

gold_validation_df = spark.createDataFrame(
    gold_validation,
    ["table_name", "row_count", "status"]
)

display(gold_validation_df)

# COMMAND ----------

latest_audit_df = (
    spark.table(audit_table)
    .orderBy(F.col("start_time").desc())
    .limit(1)
)

display(
    latest_audit_df.select(
        "run_id",
        "pipeline_name",
        "start_time",
        "end_time",
        "duration_seconds",
        "bronze_rows",
        "silver_rows",
        "reconciliation_status",
        "status"
    )
)

latest_audit_status = (
    latest_audit_df
    .select("status")
    .first()["status"]
)

print("Latest Audit Status:", latest_audit_status)

# COMMAND ----------

architecture_features = [
    ("Structured Streaming", "IMPLEMENTED"),
    ("Delta Lake", "IMPLEMENTED"),
    ("Bronze Layer", "IMPLEMENTED"),
    ("Silver Layer", "IMPLEMENTED"),
    ("Gold Layer", "IMPLEMENTED"),
    ("Event-Time Processing", "IMPLEMENTED"),
    ("Watermarking", "IMPLEMENTED"),
    ("Window Aggregations", "IMPLEMENTED"),
    ("Anomaly Detection", "IMPLEMENTED"),
    ("Data Quality", "IMPLEMENTED"),
    ("Audit Monitoring", "IMPLEMENTED"),
    ("Performance Optimization", "IMPLEMENTED"),
    ("Pipeline Orchestration", "IMPLEMENTED")
]

architecture_df = spark.createDataFrame(
    architecture_features,
    ["feature", "status"]
)

display(architecture_df)

# COMMAND ----------

failed_tables = (
    table_validation_df
    .filter(F.col("validation_status") != "PASS")
    .count()
)

failed_gold = (
    gold_validation_df
    .filter(F.col("status") != "PASS")
    .count()
)

failed_streaming = (
    streaming_validation_df
    .filter(F.col("status") != "PASS")
    .count()
)

technical_checks = [
    (
        "Project Tables",
        "PASS" if failed_tables == 0 else "FAIL"
    ),
    (
        "Layer Reconciliation",
        reconciliation_status
    ),
    (
        "Data Quality",
        dq_status
    ),
    (
        "Streaming Processing",
        "PASS" if failed_streaming == 0 else "FAIL"
    ),
    (
        "Device/Site Coverage",
        coverage_status
    ),
    (
        "Gold Layer",
        "PASS" if failed_gold == 0 else "FAIL"
    ),
    (
        "Pipeline Audit",
        "PASS" if latest_audit_status == "SUCCESS" else "FAIL"
    )
]

technical_validation_df = spark.createDataFrame(
    technical_checks,
    ["validation_check", "status"]
)

display(technical_validation_df)

# COMMAND ----------

failed_checks = (
    technical_validation_df
    .filter(F.col("status") != "PASS")
    .count()
)

total_checks = technical_validation_df.count()

passed_checks = total_checks - failed_checks

validation_percentage = round(
    (passed_checks / total_checks) * 100,
    2
) if total_checks > 0 else 0.0

final_project_status = (
    "VALIDATED"
    if failed_checks == 0
    else "VALIDATION_FAILED"
)

print("Total Checks          :", total_checks)
print("Passed Checks         :", passed_checks)
print("Failed Checks         :", failed_checks)
print("Validation Percentage :", validation_percentage, "%")
print("Final Project Status  :", final_project_status)

# COMMAND ----------

print("======================================================")
print("       REAL-TIME IoT STREAMING LAKEHOUSE")
print("             FINAL PROJECT VALIDATION")
print("======================================================")

print("Source Rows             :", source_count)
print("Bronze Rows             :", bronze_count)
print("Silver Rows             :", silver_count)
print("Watermarked Rows        :", watermark_count)
print("Window Metric Rows      :", window_count)
print("Anomaly Alert Rows      :", alert_count)

print("Devices Monitored       :", device_count)
print("Sites Monitored         :", site_count)

print("Architecture Features   :", architecture_df.count())
print("Technical Checks        :", total_checks)
print("Passed Checks           :", passed_checks)
print("Failed Checks           :", failed_checks)
print("Validation Percentage   :", validation_percentage, "%")

print("Layer Reconciliation    :", reconciliation_status)
print("Data Quality            :", dq_status)
print("Latest Pipeline Audit   :", latest_audit_status)

print("Final Project Status    :", final_project_status)
print("======================================================")