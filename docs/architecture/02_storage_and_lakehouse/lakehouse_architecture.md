# Unified Lakehouse & Analytical Storage Architecture

## 1. Overview & Objectives

The **AIOps Data Lakehouse** provides a unified, highly optimized analytical storage layer built natively on **Google Cloud Platform (GCP)**. It serves two distinct workloads:
1. **Low-Latency Analytical Querying**: Supporting real-time ML feature extraction, BQML anomaly detection models, and Gemini SRE Agent contextual queries.
2. **Cost-Effective Long-Term Telemetry Archival**: Storing petabyte-scale historical logs and traces in compressed **Apache Parquet** format for historical trend analysis and compliance auditing.

```mermaid
flowchart TD
    %% INGESTION INPUT
    Dataflow["⚙️ <b>Cloud Dataflow Ingestion</b><br/>Canonical Telemetry Stream"]

    %% STORAGE TIERS
    subgraph Lakehouse["GCP Unified Lakehouse Architecture"]
        direction TB
        subgraph HotTier["🔥 Hot Tier (0 – 30 Days)"]
            BQ_Hot[("<b>BigQuery Active Storage</b><br/>• Partitioned by <code>DATE(timestamp)</code><br/>• Clustered by <code>source_tool</code>, <code>service_name</code><br/>• Sub-second query latency for SRE Agents")]
        end

        subgraph WarmTier["⚡ Warm Tier (30 – 365 Days)"]
            BQ_Warm[("<b>BigLake & Apache Iceberg</b><br/>• Automated Long-Term Storage Pricing<br/>• BQML Training Datasets<br/>• Zero-copy query federation")]
        end

        subgraph ColdTier["🧊 Cold Tier (1 – 7 Years)"]
            GCS_Cold[("<b>Google Cloud Storage (Parquet)</b><br/>• GCS Archive Storage Class<br/>• Snappy / ZSTD compressed Parquet files<br/>• Regulatory & Compliance retention")]
        end
    end

    %% CONSUMERS
    subgraph Consumers["Analytical & AI Consumers"]
        direction LR
        BQML["📈 <b>BigQuery ML</b><br/>ARIMA_PLUS Baseline Models"]
        Gemini["🤖 <b>Gemini SRE Agent</b><br/>Diagnostic & RCA Queries"]
        Spark["⚡ <b>Dataproc / Spark</b><br/>Large-Scale ML Retraining"]
    end

    Dataflow -->|Storage Write API| BQ_Hot
    Dataflow -->|Hourly Micro-Batches| GCS_Cold
    BQ_Hot -->|Automatic Aging (30d)| BQ_Warm
    BQ_Warm -->|Lifecycle Policy (365d)| GCS_Cold

    BQ_Hot <--> BQML
    BQ_Hot <--> Gemini
    BQ_Warm <--> Spark
    GCS_Cold <--> Spark

    classDef inStyle fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px,color:#4A148C;
    classDef hotStyle fill:#FFEBEE,stroke:#C62828,stroke-width:2px,color:#B71C1C;
    classDef warmStyle fill:#FFF3E0,stroke:#E65100,stroke-width:2px,color:#BF360C;
    classDef coldStyle fill:#E3F2FD,stroke:#1565C0,stroke-width:2px,color:#0D47A1;
    classDef conStyle fill:#EDE7F6,stroke:#303F9F,stroke-width:2px,color:#1A237E;

    class Dataflow inStyle;
    class BQ_Hot hotStyle;
    class BQ_Warm warmStyle;
    class GCS_Cold coldStyle;
    class BQML,Gemini,Spark conStyle;
```

---

## 2. BigQuery Dataset & Table Design

### 2.1 Primary Analytical Table: `telemetry_canonical`
* **Table Type**: Partitioned & Clustered Columnar Table.
* **Partitioning Specification**: Partition by day on `DATE(timestamp)`.
  * Partition Expiration: Optional (managed via lifecycle tiering).
* **Clustering Keys**: `source_tool`, `entity.service_name`, `severity`, `domain`.
* **Rationale**: SRE forensic queries and ML anomaly jobs almost always filter by time range, tool, and service name. Clustering reduces bytes scanned by over 85%, ensuring fast query response times and minimal BigQuery query costs.

### 2.2 Table Schema DDL (BigQuery SQL)

```sql
CREATE OR REPLACE TABLE `aiops_lakehouse.telemetry_canonical`
(
  event_id STRING NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  ingestion_timestamp TIMESTAMP NOT NULL,
  source_tool STRING NOT NULL,
  domain STRING NOT NULL,
  severity STRING NOT NULL,
  entity STRUCT<
    service_name STRING,
    environment STRING,
    host STRING,
    container_name STRING,
    cloud_provider STRING,
    region STRING,
    cmdb_ci_id STRING
  >,
  metrics ARRAY<STRUCT<
    name STRING,
    value FLOAT64,
    unit STRING,
    dimensions JSON
  >>,
  log_payload STRUCT<
    message STRING,
    stack_trace STRING,
    trace_id STRING,
    span_id STRING,
    error_code STRING
  >,
  business_context STRUCT<
    orders_per_minute FLOAT64,
    cart_abandonment_rate FLOAT64,
    estimated_revenue_loss_usd FLOAT64,
    funnel_stage STRING,
    user_cohort STRING
  >,
  raw_attributes JSON
)
PARTITION BY DATE(timestamp)
CLUSTER BY source_tool, entity.service_name, severity;
```

---

## 3. Storage Tiering & Lifecycle Management

| Storage Tier | Data Age | Storage Technology | Storage Cost Profile | Primary Query Engine |
| :--- | :--- | :--- | :--- | :--- |
| **Hot Tier** | 0 – 30 Days | BigQuery Active Storage | Standard BigQuery Active Storage | BigQuery SQL / Vertex AI Agent |
| **Warm Tier** | 31 – 365 Days | BigLake (Iceberg / BigQuery Long-Term) | 50% discount on BigQuery storage pricing | BigQuery SQL / Dataproc Serverless |
| **Cold Tier** | 1 – 7 Years | Google Cloud Storage (Archive Class) | Ultra-low cost object storage ($0.0012/GB/mo) | BigQuery BigLake External Queries |

---

## 4. Performance & Cost Optimization Best Practices

1. **Parquet Columnar Format**: All batched data files written to Cloud Storage utilize **Apache Parquet with Snappy or ZSTD compression**, optimizing storage footprint and scan efficiency.
2. **Partition Pruning**: All analytical queries generated by the Gemini SRE Agent must include mandatory time range filters (`WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)`).
3. **BigQuery BI Engine**: Enabled with a 50 GB memory reservation to accelerate real-time dashboarding and high-frequency anomaly evaluation queries.
