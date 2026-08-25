# Real-Time IoT Streaming Lakehouse

![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-red)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-Structured%20Streaming-orange)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Medallion%20Architecture-blue)
![PySpark](https://img.shields.io/badge/PySpark-Data%20Engineering-yellow)
![Status](https://img.shields.io/badge/Project-Validated-success)

## Overview

This project implements a production-style **Real-Time IoT Streaming Lakehouse** using Databricks, PySpark, Apache Spark Structured Streaming, and Delta Lake.

The solution simulates IoT telemetry events and processes them through a Medallion Architecture consisting of Bronze, Silver, and Gold layers.

The pipeline demonstrates practical data engineering concepts including:

- Structured Streaming
- Delta Lake
- Bronze, Silver, and Gold architecture
- Incremental ingestion
- Data cleansing and transformation
- Event-time processing
- Watermarking
- Streaming window aggregations
- IoT anomaly detection
- Gold-layer business metrics
- Data quality validation
- Monitoring and audit logging
- Pipeline orchestration
- Performance optimization
- End-to-end reconciliation
- Automated project validation

---

## Architecture

```text
                IoT Devices / Sensors
                         |
                         v
              IoT Event Generation
                         |
                         v
              +--------------------+
              |    Bronze Layer    |
              | Raw Streaming Data |
              +--------------------+
                         |
                         v
              +--------------------+
              |    Silver Layer    |
              | Clean & Transform  |
              +--------------------+
                         |
                         v
                Event-Time Processing
                   + Watermarking
                         |
                         v
                Streaming Windows
                         |
                         v
                 Anomaly Detection
                         |
                         v
              +--------------------+
              |     Gold Layer     |
              | Business Metrics   |
              +--------------------+
                         |
             +-----------+-----------+
             |                       |
             v                       v
      Monitoring & Audit      Performance Tuning
             |                       |
             +-----------+-----------+
                         |
                         v
              Pipeline Orchestration
                         |
                         v
                Project Validation
```

---

## Technology Stack

| Technology | Purpose |
|---|---|
| Databricks | Lakehouse development platform |
| Apache Spark | Distributed data processing |
| PySpark | Data engineering and transformation |
| Spark Structured Streaming | Streaming data processing |
| Delta Lake | Reliable Lakehouse storage |
| SQL | Validation and analytics |
| Python | Pipeline development |
| Databricks Serverless | Compute environment |

---

## Medallion Architecture

### Bronze Layer

The Bronze layer stores raw IoT telemetry events with minimal transformation.

Responsibilities:

- Raw event ingestion
- Incremental processing
- Source preservation
- Streaming ingestion
- Traceability

### Silver Layer

The Silver layer cleans, standardizes, validates, and enriches Bronze data.

Responsibilities:

- Data type standardization
- Null handling
- Duplicate handling
- Data quality rules
- IoT event classification
- Derived attributes
- Anomaly enrichment

### Gold Layer

The Gold layer contains curated metrics suitable for analytics, monitoring, and downstream reporting.

Responsibilities:

- Device-level metrics
- Site-level metrics
- Anomaly KPIs
- Operational summaries
- Aggregated business metrics

---

## Streaming Processing

Spark Structured Streaming is used to demonstrate incremental processing of IoT events.

The streaming pipeline includes:

```text
IoT Events
    |
    v
Bronze Streaming Ingestion
    |
    v
Silver Transformation
    |
    v
Event-Time + Watermark
    |
    v
5-Minute Window Aggregation
    |
    v
Anomaly Detection
    |
    v
Gold Metrics
```

Checkpointing is used to maintain streaming processing state.

---

## Event-Time Processing and Watermarking

The project processes IoT telemetry using event timestamps rather than relying only on processing time.

A watermark is applied to handle late-arriving events while controlling streaming state.

This demonstrates an important production streaming pattern:

```text
Event Timestamp
      |
      v
Watermark
      |
      v
Window Aggregation
      |
      v
Late Event Handling
```

---

## Streaming Window Aggregation

IoT telemetry is aggregated into time windows.

Metrics include:

- Event count
- Average temperature
- Maximum temperature
- Average vibration
- Maximum vibration
- Average pressure
- Maximum pressure
- Anomaly count

The project uses window-based processing to transform event-level telemetry into operational metrics.

---

## Anomaly Detection

The pipeline identifies abnormal IoT sensor behavior and creates anomaly indicators.

Example monitoring dimensions include:

- Temperature
- Vibration
- Pressure
- Device condition
- Site condition

Detected anomalies are propagated into downstream monitoring and Gold-layer metrics.

---

## Monitoring and Audit Framework

The project contains a monitoring and audit layer to track pipeline execution.

The framework demonstrates:

- Pipeline status
- Layer reconciliation
- Data quality status
- Processing statistics
- Alert information
- Audit history
- Pipeline health

This provides operational visibility into the Lakehouse pipeline.

---

## Performance Optimization

The project demonstrates Spark optimization concepts including:

- Efficient DataFrame transformations
- Broadcast joins
- Selective column processing
- Aggregation optimization
- Execution-plan awareness
- Serverless-compatible optimization patterns

These techniques represent common approaches used to improve Spark workload performance.

---

## Pipeline Orchestration

The pipeline is organized into multiple logical stages.

```text
01 Source Generation
        |
        v
02 Bronze Ingestion
        |
        v
03 Silver Transformation
        |
        v
04 Event-Time Watermarking
        |
        v
05 Window Aggregation
        |
        v
06 Anomaly Detection
        |
        v
07 Gold Metrics
        |
        v
08 Monitoring & Audit
        |
        v
09 Performance Optimization
        |
        v
10 Pipeline Orchestration
        |
        v
11 Project Validation
```

---

## Project Notebooks

| # | Notebook | Purpose |
|---|---|---|
| 01 | `01_IoT_Source_Generation.py` | Generates IoT telemetry source events |
| 02 | `02_Bronze_Streaming_Ingestion.py` | Performs Bronze streaming ingestion |
| 03 | `03_Silver_Streaming_Transformation.py` | Cleans and transforms Bronze data |
| 04 | `04_Event_Time_Watermarking.py` | Implements event-time processing and watermarking |
| 05 | `05_Streaming_Window_Aggregations.py` | Creates streaming window metrics |
| 06 | `06_Streaming_Anomaly_Detection.py` | Detects abnormal IoT telemetry |
| 07 | `07_Gold_Streaming_Metrics.py` | Builds curated Gold metrics |
| 08 | `08_Streaming_Monitoring_and_Audit.py` | Implements monitoring and audit controls |
| 09 | `09_Streaming_Performance_Optimization.py` | Demonstrates Spark optimization techniques |
| 10 | `10_Pipeline_Orchestration.py` | Coordinates pipeline stages |
| 11 | `11_Project_Validation.py` | Performs end-to-end validation |

---

## Data Quality

The Silver processing layer applies data quality controls before records are consumed downstream.

Validation includes:

- Required-field validation
- Duplicate handling
- Sensor value validation
- Transformation checks
- Layer reconciliation
- Pipeline health checks

This helps prevent invalid telemetry from silently propagating into analytics layers.

---

## End-to-End Validation

The final validation notebook performs technical checks across the complete Lakehouse.

Final validation result:

```text
REAL-TIME IOT STREAMING LAKEHOUSE
FINAL PROJECT VALIDATION

Source Rows             : 55
Bronze Rows             : 55
Silver Rows             : 55
Watermarked Rows        : 55
Window Metric Rows      : 11
Anomaly Alert Rows      : 4
Devices Monitored       : 5
Sites Monitored         : 3
Architecture Features   : 13

Technical Checks        : 7
Passed Checks           : 7
Failed Checks           : 0
Validation Percentage   : 100.0 %

Layer Reconciliation    : PASS
Data Quality            : PASS
Latest Pipeline Audit   : SUCCESS

Final Project Status    : VALIDATED
```

This confirms successful execution and reconciliation of the implemented pipeline stages.

---

## Execution Evidence

### IoT Source Generation

![IoT Source Events](01_iot_source_events.png)

![IoT Source Summary](02_iot_source_summary.png)

### Bronze Streaming Layer

![Bronze Streaming Source](03_bronze_streaming_source.png)

![Bronze Incremental Streaming](04_bronze_incremental_streaming.png)

![Bronze Streaming Summary](05_bronze_streaming_summary.png)

### Silver Transformation

![Silver Anomaly Enrichment](06_silver_anomaly_enrichment.png)

![Silver Reconciliation](07_silver_reconciliation.png)

![Silver Transformation Summary](08_silver_transformation_summary.png)

### Event-Time Processing

![Late Event Classification](09_late_event_classification.png)

![Watermark Deduplication](10_watermark_deduplication.png)

![Event Time Summary](11_event_time_summary.png)

### Streaming Window Aggregation

![Streaming Window Metrics](12_streaming_window_metrics.png)

![High Risk Windows](13_high_risk_windows.png)

![Window Reconciliation](14_window_reconciliation.png)

![Streaming Window Summary](15_streaming_window_summary.png)

### Anomaly Detection

![Anomaly Alert Candidates](16_anomaly_alert_candidates.png)

![Anomaly Validation](17_anomaly_validation.png)

![Anomaly Detection Summary](18_anomaly_detection_summary.png)

### Gold Layer

![Gold Top Risk Devices](19_gold_top_risk_devices.png)

![Gold Tables Validation](20_gold_tables_validation.png)

![Gold Streaming Summary](21_gold_streaming_summary.png)

### Monitoring and Audit

![Pipeline Audit History](22_pipeline_audit_history.png)

![Pipeline Health Validation](23_pipeline_health_validation.png)

![Monitoring Audit Summary](24_monitoring_audit_summary.png)

### Performance Optimization

![Photon Execution Plan](25_photon_execution_plan.png)

![Performance Validation](26_performance_validation.png)

![Performance Optimization Summary](27_performance_optimization_summary.png)

### Pipeline Orchestration

![Pipeline Stages](28_pipeline_stages.png)

![Orchestration Validation](29_orchestration_validation.png)

![Pipeline Orchestration Summary](30_pipeline_orchestration_summary.png)

### Final Validation

![Architecture Features](31_architecture_features.png)

![Project Validation](32_project_validation_100_percent.png)

---

## Key Engineering Concepts Demonstrated

This project demonstrates practical implementation of:

1. Lakehouse Architecture
2. Medallion Architecture
3. Structured Streaming
4. Delta Lake
5. Incremental Processing
6. Event-Time Processing
7. Watermarking
8. Window Aggregations
9. Streaming Anomaly Detection
10. Data Quality Engineering
11. Pipeline Monitoring
12. Audit Logging
13. Spark Performance Optimization
14. Pipeline Orchestration
15. End-to-End Validation

---

## Repository Structure

```text
real-time-iot-streaming-lakehouse/
│
├── 01_IoT_Source_Generation.py
├── 02_Bronze_Streaming_Ingestion.py
├── 03_Silver_Streaming_Transformation.py
├── 04_Event_Time_Watermarking.py
├── 05_Streaming_Window_Aggregations.py
├── 06_Streaming_Anomaly_Detection.py
├── 07_Gold_Streaming_Metrics.py
├── 08_Streaming_Monitoring_and_Audit.py
├── 09_Streaming_Performance_Optimization.py
├── 10_Pipeline_Orchestration.py
├── 11_Project_Validation.py
│
├── 01_iot_source_events.png
├── 02_iot_source_summary.png
├── ...
├── 31_architecture_features.png
├── 32_project_validation_100_percent.png
│
└── README.md
```

---

## How to Run

Run the notebooks sequentially in the following order:

```text
01_IoT_Source_Generation
02_Bronze_Streaming_Ingestion
03_Silver_Streaming_Transformation
04_Event_Time_Watermarking
05_Streaming_Window_Aggregations
06_Streaming_Anomaly_Detection
07_Gold_Streaming_Metrics
08_Streaming_Monitoring_and_Audit
09_Streaming_Performance_Optimization
10_Pipeline_Orchestration
11_Project_Validation
```

After execution, the final validation notebook should be used to verify the pipeline status and reconciliation results.

---

## Project Outcome

The completed solution demonstrates an end-to-end streaming Lakehouse workflow from IoT telemetry generation through ingestion, transformation, event-time processing, anomaly detection, Gold-layer aggregation, monitoring, optimization, orchestration, and technical validation.

The project was successfully validated with:

- **100% validation**
- **7/7 technical checks passed**
- **0 failed checks**
- **Layer reconciliation: PASS**
- **Data quality: PASS**
- **Pipeline audit: SUCCESS**
- **Final project status: VALIDATED**

---

## Author

**Gunasekaran Ravi**

Azure Data Engineer | Databricks | PySpark | Delta Lake | SQL | Data Engineering
