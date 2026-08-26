# Unified Lakehouse & Analytical Storage Architecture

## 1. Overview & Objectives

The **AIOps Data Lakehouse** provides a unified, highly optimized analytical storage layer built natively on **Google Cloud Platform (GCP)**. It serves three distinct workloads essential to the AIOps Intelligence Layer:
1. **Low-Latency Analytical Querying**: Supporting real-time ML feature extraction, BQML anomaly detection models, and Gemini SRE Agent contextual queries.
2. **Cost-Effective Long-Term Telemetry Archival**: Storing petabyte-scale historical logs and traces in compressed **Apache Parquet** format for historical trend analysis and compliance auditing.
3. **Semantic Retrieval-Augmented Generation (RAG)**: Storing vectorized representations of Runbooks (SOPs) and historical incident data for real-time retrieval by the Gemini SRE Agent.

```mermaid
flowchart TD
    %% INGESTION INPUT
    Dataflow["⚙️ <b>Cloud Dataflow Ingestion</b><br/>Canonical Telemetry Stream"]
    GHA["🐙 <b>GitHub Actions</b><br/>Markdown Runbook Sync"]

    %% STORAGE TIERS
    subgraph Lakehouse["GCP Unified Lakehouse Architecture"]
        direction TB
        subgraph HotTier["🔥 Hot Tier (0 – 30 Days)"]
            BQ_Hot[("<b>BigQuery Active Storage</b><br/>• Partitioned & Clustered<br/>• Sub-second query latency")]
            BQ_MV[("<b>BigQuery Materialized Views</b><br/>• Real-time Metric Rollups<br/>• Feeds Feature Store")]
        end

        subgraph WarmTier["⚡ Warm Tier (30 – 365 Days)"]
            BQ_Warm[("<b>BigLake & Apache Iceberg</b><br/>• Automated Pricing<br/>• BQML Training Datasets")]
        end

        subgraph ColdTier["🧊 Cold Tier (1 – 7 Years)"]
            GCS_Cold[("<b>Google Cloud Storage (Parquet)</b><br/>• GCS Archive Storage<br/>• Compliance retention")]
        end
        
        subgraph VectorDB["🧠 Vector Lakehouse (RAG)"]
            Vertex[("<b>Vertex AI Vector Search</b><br/>• Embedding Indexes<br/>• High-QPS Semantic Search")]
            GCS_Docs[("<b>Cloud Storage (Unstructured)</b><br/>• Raw SOP Markdown")]
        end
    end

    %% CONSUMERS
    subgraph Consumers["Analytical & AI Consumers"]
        direction TB
        BQML["📈 <b>BigQuery ML</b><br/>ARIMA_PLUS Baseline Models"]
        Gemini["🤖 <b>Gemini SRE Agent</b><br/>Diagnostic & RCA Queries"]
        Router["🧠 <b>Semantic Router (LLM)</b><br/>Alert Correlation & Triage"]
        Spark["⚡ <b>Dataproc / Spark</b><br/>Large-Scale ML Retraining"]
    end

    Dataflow -->|Storage Write API| BQ_Hot
    Dataflow -->|Hourly Micro-Batches| GCS_Cold
    GHA -->|Push Markdown| GCS_Docs
    GCS_Docs -.->|Vertex Embedding API| Vertex

    BQ_Hot -->|Automatic Aging (30d)| BQ_Warm
    BQ_Hot -.->|Pre-compute Rollups| BQ_MV
    BQ_Warm -->|Lifecycle Policy (365d)| GCS_Cold

    BQ_MV <--> BQML
    BQ_Hot <--> Router
    BQ_Hot <--> Gemini
    Vertex <-->|Semantic Search| Gemini
    BQ_Warm <--> Spark
    GCS_Cold <--> Spark

    classDef inStyle fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px,color:#4A148C;
    classDef hotStyle fill:#FFEBEE,stroke:#C62828,stroke-width:2px,color:#B71C1C;
    classDef warmStyle fill:#FFF3E0,stroke:#E65100,stroke-width:2px,color:#BF360C;
    classDef coldStyle fill:#E3F2FD,stroke:#1565C0,stroke-width:2px,color:#0D47A1;
    classDef vecStyle fill:#E0F2F1,stroke:#00695C,stroke-width:2px,color:#004D40;
    classDef conStyle fill:#EDE7F6,stroke:#303F9F,stroke-width:2px,color:#1A237E;

    class Dataflow,GHA inStyle;
    class BQ_Hot,BQ_MV hotStyle;
    class BQ_Warm warmStyle;
    class GCS_Cold coldStyle;
    class Vertex,GCS_Docs vecStyle;
    class BQML,Gemini,Router,Spark conStyle;
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

### 2.3 Real-Time AI Feature Store & Materialized Views
To power the `ARIMA_PLUS` models in **BigQuery ML (BQML)** without scanning the massive `telemetry_canonical` table every minute, we utilize **BigQuery Materialized Views**. These act as a continuously updated Feature Store for the ML engine.

By pre-aggregating high-frequency metrics (like Orders Per Minute), the Semantic Router and BQML can evaluate anomalies in sub-seconds with near-zero cost.

```sql
-- Materialized View for 1-minute OPM (Orders Per Minute) Rollups
CREATE MATERIALIZED VIEW `aiops_lakehouse.telemetry_opm_1min_mv`
PARTITION BY DATE(window_start)
CLUSTER BY service_name
AS
SELECT
  TIMESTAMP_TRUNC(timestamp, MINUTE) AS window_start,
  entity.service_name,
  SUM(business_context.orders_per_minute) AS total_opm,
  AVG(business_context.cart_abandonment_rate) AS avg_abandon_rate
FROM
  `aiops_lakehouse.telemetry_canonical`
WHERE
  source_tool = 'adobe_analytics'
  AND business_context.orders_per_minute IS NOT NULL
GROUP BY
  window_start,
  service_name;
```

---

## 3. Storage Tiering & Lifecycle Management

| Storage Tier | Data Age | Storage Technology | Storage Cost Profile | Primary Query Engine |
| :--- | :--- | :--- | :--- | :--- |
| **Hot Tier** | 0 – 30 Days | BigQuery Active Storage | Standard BigQuery Active Storage | BigQuery SQL / Vertex AI Agent |
| **Warm Tier** | 31 – 365 Days | BigLake (Iceberg / BigQuery Long-Term) | 50% discount on BigQuery storage pricing | BigQuery SQL / Dataproc Serverless |
| **Cold Tier** | 1 – 7 Years | Google Cloud Storage (Archive Class) | Ultra-low cost object storage ($0.0012/GB/mo) | BigQuery BigLake External Queries |

---

## 4. Vector Lakehouse & RAG Storage

To support the **Context-Aware Incident Response** requirement of the Intelligence Layer, unstructured text must be semantically searchable. This requires a dedicated Vector Lakehouse architecture parallel to the relational BigQuery tables.

### 4.1 Storage Pipeline
1. **Raw Storage**: Markdown Runbooks (SOPs) are managed in GitHub ("Runbooks as Code"). A GitHub Actions pipeline syncs merged Markdown files directly to a standard **Google Cloud Storage (GCS) Bucket**.
2. **Embedding**: A Cloud Function triggers on new GCS files, chunks the Markdown, and calls the **Vertex AI Text Embeddings API** to generate high-dimensional vectors.
3. **Vector Database**: The resulting vectors, along with metadata (e.g., `service_name: checkout-service`, `author: sre-pod-1`), are indexed in **Vertex AI Vector Search**.

### 4.2 Retrieval
When the Gemini SRE Agent begins diagnosing a P1 incident, it extracts the `service_name` and `symptoms` from the alert payload. It executes an Approximate Nearest Neighbor (ANN) search against Vertex AI Vector Search to retrieve the exact Markdown SOP required for remediation within milliseconds.

---

## 5. Integration with Semantic Intelligence Router

The **Semantic Intelligence Router (LLM)** relies heavily on the Hot Tier of this Lakehouse. 

When a burst of alerts arrives in Pub/Sub (e.g., 50 alerts from Splunk, 10 from Akamai), the Router uses **Function Calling (Tools)** to query BigQuery securely.
* The Router executes dynamic SQL against `aiops_lakehouse.telemetry_canonical` to check if a recent deployment occurred (via Audit Logs).
* It queries the Materialized Views (`telemetry_opm_1min_mv`) to verify if the technical alerts are actually impacting business conversion rates.
* By having this data pre-clustered and materialized, the LLM can pull cross-platform context within 2 seconds before creating a unified, enriched ticket in ServiceNow.

---

## 6. Performance & Cost Optimization Best Practices

1. **Parquet Columnar Format**: All batched data files written to Cloud Storage utilize **Apache Parquet with Snappy or ZSTD compression**, optimizing storage footprint and scan efficiency.
2. **Partition Pruning**: All analytical queries generated by the Gemini SRE Agent and Semantic Router must include mandatory time range filters (`WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)`).
3. **BigQuery BI Engine**: Enabled with a 50 GB memory reservation to accelerate real-time Materialized View reads and high-frequency anomaly evaluation queries by BQML.
