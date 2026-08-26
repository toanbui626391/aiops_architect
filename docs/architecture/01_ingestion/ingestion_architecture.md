# Ingestion Layer & Streaming Pipelines Architecture

This document outlines the **Enterprise AIOps Ingestion Engine**, which captures telemetry from various observability sources, processes it in real-time, and routes it to downstream analytics and incident response systems.

---

## 1. Simplified Architecture Overview

The architecture follows a streamlined, connector-based approach to securely ingest data from hybrid and multi-cloud environments. The design ensures high throughput, strict security (via API Keys), and real-time processing.

```mermaid
flowchart TD
    %% Sources
    Sources[("<b>Observability Sources</b><br/>(Akamai, Dynatrace, Splunk, Adobe, Legacy, GCP)")]
    
    %% Connectors
    subgraph Connectors ["1. Ingestion Connector Layer (GCP)"]
        direction TB
        Push["⚙️ <b>Push Connectors (Cloud Run)</b><br/>API Key Auth for Modern SaaS"]
        Pull["🏃‍♂️ <b>Pull Connectors (Cloud Composer)</b><br/>Airflow DAGs for Legacy DBs"]
        Native["🚦 <b>Native Connectors (Log Router)</b><br/>Direct Routing for GCP Ops"]
    end
    
    %% Event Bus
    PubSub["📬 <b>Cloud Pub/Sub</b><br/>Isolated Topics (e.g., telemetry.akamai.raw)"]
    
    %% Stream Processing
    Dataflow["⚙️ <b>Cloud Dataflow (Apache Beam)</b><br/>Deduplication, DLP Scrubbing & Normalization"]
    
    %% Storage Sinks
    subgraph Storage ["2. Storage & Action Sinks"]
        direction TB
        BQ[("🗄️ <b>BigQuery</b><br/>Analytics Lakehouse")]
        GCS[("📦 <b>Cloud Storage</b><br/>Parquet Cold Archive")]
        Action["🧠 <b>Vertex AI & ServiceNow</b><br/>Automated Incident Response"]
    end

    %% Flow
    Sources --> Push
    Sources --> Pull
    Sources --> Native
    
    Push --> PubSub
    Pull --> PubSub
    Native --> PubSub
    
    PubSub --> Dataflow
    
    Dataflow --> BQ
    Dataflow --> GCS
    Dataflow --> Action

    %% Styling
    classDef srcStyle fill:#ECEFF1,stroke:#37474F,stroke-width:2px;
    classDef conStyle fill:#E3F2FD,stroke:#1565C0,stroke-width:2px;
    classDef psStyle fill:#FFF3E0,stroke:#E65100,stroke-width:2px;
    classDef dfStyle fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px;
    classDef sinkStyle fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px;

    class Sources srcStyle;
    class Push,Pull,Native conStyle;
    class PubSub psStyle;
    class Dataflow dfStyle;
    class BQ,GCS,Action sinkStyle;
```

---

## 2. The Connector Architecture

We utilize three specialized connector patterns to ingest telemetry securely into GCP. 

### 2.1 Push Connectors (Modern SaaS & Edge)
Used by sources that can push webhooks in real-time (**Akamai**, **Dynatrace**, **Splunk HEC**, **Adobe Analytics**).
* **Ingress**: Traffic enters via **Cloud Armor WAF** (IP Allowlisting & Rate Limiting).
* **Compute**: Stateless **Cloud Run** services.
* **Authentication**: Requires secure **API Keys** passed in HTTP headers (`Authorization: Bearer <API_KEY>`). The keys are validated in real-time against **GCP Secret Manager**.
* **Action**: Batches validated payloads and publishes to Cloud Pub/Sub.

### 2.2 Pull Connectors (Legacy & Air-Gapped Systems)
Used by on-premise relational databases or air-gapped systems that cannot push outbound traffic.
* **Orchestration**: **Cloud Composer (Apache Airflow)** schedules and runs Directed Acyclic Graphs (DAGs) to periodically extract data.
* **Network**: Queries run securely over a **Cloud VPN** or **Cloud Interconnect**.
* **Action**: Uses pre-built Airflow operators to extract delta records, convert them to JSON, and publish to Cloud Pub/Sub (or drop into Cloud Storage for downstream processing).
* **Advantage**: Easier to maintain, debug, and monitor complex data extraction pipelines using the Airflow UI, with a massive ecosystem of pre-built operators for legacy systems.

### 2.3 Native Connectors (GCP Operations Suite)
Used natively by **Google Kubernetes Engine (GKE)** and **Cloud Audit Logs**.
* **Mechanism**: Leverages **Cloud Logging Log Router Sinks** to route logs natively.
* **Advantage**: Zero-compute ingestion with sub-second latency and no egress costs.

---

## 3. Decoupled Buffer & Stream Processing

### 3.1 Cloud Pub/Sub (The Event Bus)
* **Isolation**: Each connector publishes to a dedicated topic (e.g., `telemetry.akamai.raw`). This prevents traffic spikes in one source from impacting others.
* **Resilience**: Features Dead-Letter Queues (DLQ) to catch and isolate unparseable or malformed payloads.

### 3.2 Cloud Dataflow (Apache Beam)
A unified streaming pipeline processes all telemetry in real-time:
1. **Deduplication**: Removes duplicate events using a 10-minute sliding window.
2. **Hybrid DLP Engine**: Masks sensitive PII/PCI data using fast in-memory regex (for credit cards/SSNs) and the Cloud DLP API (for unstructured text).
3. **Normalization**: Converts all varied telemetry formats into a standard canonical schema.
4. **Routing**: Delivers clean data to BigQuery, GCS, and high-severity alerts to ServiceNow.

---

## 4. Sub-Modules & Detailed Guides

For granular details on specific source connectors, refer to the individual connector documentation:
* [Akamai DataStream Connector](connectors/akamai_datastream.md)
* [Dynatrace Ingestion Connector](connectors/dynatrace_ingestion.md)
* [GCP Operations Ingestion Connector](connectors/gcp_ops_ingestion.md)
* [Splunk HEC Connector](connectors/splunk_hec_ingestion.md)
* [Adobe Analytics Streaming Connector](connectors/adobe_analytics_stream.md)
