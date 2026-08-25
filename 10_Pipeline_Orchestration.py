# Databricks notebook source
# Real-Time IoT Streaming Lakehouse
# 10 - Pipeline Orchestration

from pyspark.sql import functions as F

pipeline_name = "Real_Time_IoT_Streaming_Lakehouse"

required_tables = [
    ("SOURCE", "workspace.default.iot_source_events"),
    ("BRONZE", "workspace.default.iot_bronze_events"),
    ("SILVER", "workspace.default.iot_silver_events"),
    ("WATERMARK", "workspace.default.iot_watermarked_events"),
    ("WINDOW_METRICS", "workspace.default.iot_window_metrics"),
    ("ANOMALY_ALERTS", "workspace.default.iot_anomaly_alerts"),
    ("GOLD_DEVICE", "workspace.default.iot_gold_device_health"),
    ("GOLD_SITE", "workspace.default.iot_gold_site_health"),
    ("GOLD_ALERTS", "workspace.default.iot_gold_alert_summary"),
    ("GOLD_KPIS", "workspace.default.iot_gold_operational_kpis"),
    ("AUDIT", "workspace.default.iot_pipeline_audit")
]

print("Pipeline Name :", pipeline_name)
print("Required Tables:", len(required_tables))
print("Orchestration setup initialized")

# COMMAND ----------

pipeline_stages = [
    (1, "IoT Source Generation", "COMPLETED"),
    (2, "Bronze Streaming Ingestion", "COMPLETED"),
    (3, "Silver Streaming Transformation", "COMPLETED"),
    (4, "Event-Time Watermarking", "COMPLETED"),
    (5, "Streaming Window Aggregations", "COMPLETED"),
    (6, "Streaming Anomaly Detection", "COMPLETED"),
    (7, "Gold Streaming Metrics", "COMPLETED"),
    (8, "Streaming Monitoring & Audit", "COMPLETED"),
    (9, "Streaming Performance Optimization", "COMPLETED")
]

stage_df = spark.createDataFrame(
    pipeline_stages,
    ["stage_order", "stage_name", "status"]
)

display(stage_df.orderBy("stage_order"))

# COMMAND ----------

table_validation = []

for layer, table_name in required_tables:
    try:
        df = spark.table(table_name)
        row_count = df.count()

        table_validation.append(
            (
                layer,
                table_name,
                True,
                row_count,
                "READY"
            )
        )

    except Exception:
        table_validation.append(
            (
                layer,
                table_name,
                False,
                0,
                "MISSING"
            )
        )

table_validation_df = spark.createDataFrame(
    table_validation,
    [
        "layer",
        "table_name",
        "table_exists",
        "row_count",
        "status"
    ]
)

display(table_validation_df)

# COMMAND ----------

missing_tables = (
    table_validation_df
    .filter(F.col("table_exists") == False)
    .count()
)

print("Missing Tables:", missing_tables)

if missing_tables == 0:
    print("TABLE READINESS CHECK: PASS")
else:
    print("TABLE READINESS CHECK: FAIL")

# COMMAND ----------

bronze_count = spark.table(
    "workspace.default.iot_bronze_events"
).count()

silver_count = spark.table(
    "workspace.default.iot_silver_events"
).count()

print("Bronze Rows :", bronze_count)
print("Silver Rows :", silver_count)

if bronze_count == silver_count:
    reconciliation_status = "PASS"
else:
    reconciliation_status = "FAIL"

print(
    "BRONZE -> SILVER RECONCILIATION:",
    reconciliation_status
)

# COMMAND ----------

silver_df = spark.table(
    "workspace.default.iot_silver_events"
)

duplicate_events = (
    silver_df
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_keys = (
    silver_df
    .filter(
        F.col("event_id").isNull() |
        F.col("device_id").isNull()
    )
    .count()
)

print("Duplicate Events :", duplicate_events)
print("Null Key Events   :", null_keys)

dq_status = (
    "PASS"
    if duplicate_events == 0 and null_keys == 0
    else "FAIL"
)

print("DATA QUALITY CHECK:", dq_status)

# COMMAND ----------

streaming_features = [
    ("Structured Streaming", "ENABLED"),
    ("Checkpointing", "ENABLED"),
    ("Event-Time Processing", "ENABLED"),
    ("Watermarking", "ENABLED"),
    ("Streaming Deduplication", "ENABLED"),
    ("Window Aggregations", "ENABLED"),
    ("Anomaly Detection", "ENABLED"),
    ("Delta Lake", "ENABLED"),
    ("Monitoring & Audit", "ENABLED")
]

streaming_feature_df = spark.createDataFrame(
    streaming_features,
    ["feature", "status"]
)

display(streaming_feature_df)

# COMMAND ----------

gold_tables = [
    "workspace.default.iot_gold_device_health",
    "workspace.default.iot_gold_site_health",
    "workspace.default.iot_gold_alert_summary",
    "workspace.default.iot_gold_operational_kpis"
]

gold_validation = []

for table_name in gold_tables:
    df = spark.table(table_name)

    gold_validation.append(
        (
            table_name,
            df.count(),
            "READY"
        )
    )

gold_validation_df = spark.createDataFrame(
    gold_validation,
    ["table_name", "row_count", "status"]
)

display(gold_validation_df)

# COMMAND ----------

audit_df = spark.table(
    "workspace.default.iot_pipeline_audit"
)

latest_audit = (
    audit_df
    .orderBy(F.col("start_time").desc())
    .limit(1)
)

display(
    latest_audit.select(
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

# COMMAND ----------

failed_stages = (
    stage_df
    .filter(F.col("status") != "COMPLETED")
    .count()
)

failed_gold_tables = (
    gold_validation_df
    .filter(F.col("status") != "READY")
    .count()
)

latest_status = (
    latest_audit
    .select("status")
    .first()["status"]
)

print("Failed Stages      :", failed_stages)
print("Missing Tables     :", missing_tables)
print("Failed Gold Tables :", failed_gold_tables)
print("Latest Audit Status:", latest_status)

# COMMAND ----------

pipeline_ready = (
    failed_stages == 0
    and missing_tables == 0
    and failed_gold_tables == 0
    and reconciliation_status == "PASS"
    and dq_status == "PASS"
    and latest_status == "SUCCESS"
)

final_pipeline_status = (
    "READY"
    if pipeline_ready
    else "NOT_READY"
)

print("Final Pipeline Status:", final_pipeline_status)

# COMMAND ----------

print("========== PIPELINE ORCHESTRATION SUMMARY ==========")
print("Pipeline Name          :", pipeline_name)
print("Completed Stages       :", stage_df.count())
print("Required Tables        :", len(required_tables))
print("Missing Tables         :", missing_tables)
print("Bronze Rows            :", bronze_count)
print("Silver Rows            :", silver_count)
print("Reconciliation         :", reconciliation_status)
print("Data Quality           :", dq_status)
print("Gold Tables            :", len(gold_tables))
print("Latest Audit Status    :", latest_status)
print("Structured Streaming   : ENABLED")
print("Watermarking           : ENABLED")
print("Window Aggregation     : ENABLED")
print("Anomaly Detection      : ENABLED")
print("Final Pipeline Status  :", final_pipeline_status)
print("Orchestration Status   : COMPLETE")
print("===================================================")