# End-to-End Ingestion & Streaming Pipelines

This document serves as the authoritative architectural blueprint for the **Enterprise AIOps Ingestion & Stream Processing Engine**. It specifies the end-to-end telemetry journey: from first-mile source extraction across hybrid and multi-cloud environments, through secure ingress gateways and Pub/Sub event-bus buffering, to real-time Apache Beam stream processing on Google Cloud Dataflow.

---

## 1. First-Mile Ingestion Architecture (Source to Cloud)

The AIOps platform ingests high-frequency metrics, traces, edge logs, and alerts from 5 primary observability systems plus legacy on-premise monitoring infrastructure.

```mermaid
flowchart TD
    %% 1. PRODUCERS
    subgraph Sources["1. Heterogeneous Observability Sources"]
        direction TB
        Akamai["Akamai Edge (DataStream 2)"]
        Dyna["Dynatrace SaaS (Davis AI & PurePath)"]
        Splunk["Splunk Enterprise / SIEM (HEC)"]
        Adobe["Adobe Analytics (AEP Streaming & Clickstream)"]
        Legacy["Legacy On-Premise Monitoring / Databases"]
        GCPOps["GCP Native (GKE, Audit Logs, Compute Engine)"]
    end

    %% 2. NETWORK & SECURITY PERIMETER
    subgraph Ingress_Perimeter["2. First-Mile Ingress & Security Perimeter"]
        direction TB
        CloudArmor["Cloud Armor WAF (IP Allowlist & Rate Limiting)"]
        ExtLB["Global External Application Load Balancer"]
        VPN["Cloud Interconnect / Cloud VPN (Private Transit)"]
        CloudRunGW["Cloud Run Ingestion Gateway Fleet (Auto-scaled)"]
        SecretMgr[("GCP Secret Manager (API Key Vault)")]
        CloudSched["Cloud Scheduler (Cron Trigger)"]
        PollerJob["Cloud Run Poller Job (API / JDBC Pagination)"]
        LogRouter["GCP Cloud Logging (Log Router Sinks)"]
    end

    %% 3. EVENT BUS
    subgraph Event_Bus["3. Cloud Pub/Sub Shock Absorber Fleet"]
        direction TB
        T_Akamai["telemetry.akamai.raw"]
        T_Dyna["telemetry.dynatrace.raw"]
        T_Splunk["telemetry.splunk.raw"]
        T_Adobe["telemetry.adobe.raw"]
        T_Legacy["telemetry.legacy.raw"]
        T_GCP["telemetry.gcp.raw"]
        DLQ_Fleet["telemetry.*.dlq (Dead-Letter Queues)"]
    end

    %% 4. STREAM ETL
    subgraph Dataflow_Core["4. Cloud Dataflow (Apache Beam) Stream Processing"]
        direction TB
        BeamPipeline["Apache Beam 6-Stage Streaming Topology"]
    end

    %% 5. OUTPUTS
    subgraph Storage_Sinks["5. Ingestion Storage Sinks & Egress"]
        direction TB
        BQ_Table[("BigQuery Lakehouse (Partitioned & Clustered)")]
        GCS_Parquet[("GCS Cold Storage (Compressed Parquet)")]
        Actionable_Topic["Pub/Sub: aiops.alerts.actionable (To Vertex AI & ServiceNow)"]
        Late_Data_Sink[("GCS Late-Data Archive")]
    end

    %% CONNECTIONS
    Akamai -->|"Public HTTPS POST"| CloudArmor
    Dyna -->|"Public HTTPS Webhook"| CloudArmor
    Adobe -->|"Streaming HTTP Push"| CloudArmor
    Splunk -->|"HEC Event Forwarding"| CloudArmor

    CloudArmor --> ExtLB
    ExtLB --> CloudRunGW
    CloudRunGW -.->|"Validate Bearer Token"| SecretMgr
    
    Legacy -->|"Private Query"| VPN
    VPN --> PollerJob
    CloudSched -->|"Trigger (Every 1m)"| PollerJob

    GCPOps -->|"Direct Internal Route"| LogRouter

    CloudRunGW -->|"Batched Async Publish"| T_Akamai
    CloudRunGW -->|"Batched Async Publish"| T_Dyna
    CloudRunGW -->|"Batched Async Publish"| T_Splunk
    CloudRunGW -->|"Batched Async Publish"| T_Adobe
    PollerJob -->|"Batched Publish"| T_Legacy
    LogRouter -->|"Native Pub/Sub Sink"| T_GCP

    T_Akamai --> BeamPipeline
    T_Dyna --> BeamPipeline
    T_Splunk --> BeamPipeline
    T_Adobe --> BeamPipeline
    T_Legacy --> BeamPipeline
    T_GCP --> BeamPipeline

    T_Akamai -.->|"Max Retries Exceeded"| DLQ_Fleet
    T_Dyna -.->|"Max Retries Exceeded"| DLQ_Fleet
    T_Splunk -.->|"Max Retries Exceeded"| DLQ_Fleet
    T_Adobe -.->|"Max Retries Exceeded"| DLQ_Fleet

    BeamPipeline -->|"Stream Canonical Records"| BQ_Table
    BeamPipeline -->|"Hourly Batch Parquet"| GCS_Parquet
    BeamPipeline -->|"High-Severity Actionable Alerts"| Actionable_Topic
    BeamPipeline -.->|"Late Data (>15m Lateness)"| Late_Data_Sink

    %% STYLING
    classDef srcStyle fill:#ECEFF1,stroke:#37474F,stroke-width:2px,color:#263238;
    classDef secStyle fill:#E3F2FD,stroke:#1565C0,stroke-width:2px,color:#0D47A1;
    classDef psStyle fill:#FFF3E0,stroke:#E65100,stroke-width:2px,color:#BF360C;
    classDef dfStyle fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px,color:#4A148C;
    classDef sinkStyle fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px,color:#1B5E20;

    class Akamai,Dyna,Splunk,Adobe,Legacy,GCPOps srcStyle;
    class CloudArmor,ExtLB,VPN,CloudRunGW,SecretMgr,CloudSched,PollerJob,LogRouter secStyle;
    class T_Akamai,T_Dyna,T_Splunk,T_Adobe,T_Legacy,T_GCP,DLQ_Fleet psStyle;
    class BeamPipeline dfStyle;
    class BQ_Table,GCS_Parquet,Actionable_Topic,Late_Data_Sink sinkStyle;
```

---

### 1.1 Ingestion Patterns & Transport Protocols

To accommodate varied source capabilities, telemetry flows into GCP via three specialized patterns:

#### Pattern A: Push-Based Ingestion (Modern SaaS & Edge)
Used by **Akamai DataStream 2**, **Dynatrace**, **Splunk HEC**, and **Adobe Analytics**.
* **Transport**: HTTPS `POST` requests sending compressed JSON or newline-delimited JSON payloads.
* **Edge Security**: Traffic enters via **Cloud Armor WAF** attached to a Global External Application Load Balancer. Cloud Armor enforces:
  - **Source IP Allowlisting**: Restricted to verified egress CIDR ranges of Akamai, Dynatrace, and Adobe infrastructure.
  - **Token Bucket Rate Limiting**: Max 50,000 requests/second per source CIDR to prevent volumetric DoS attacks.
* **Ingress Gateway Fleet**: Stateless, containerized **Cloud Run services** that:
  1. Inspect the `Authorization: Bearer <API_KEY>` header.
  2. Verify credentials against local in-memory cache synchronized with **GCP Secret Manager**.
  3. Buffer incoming requests and perform high-performance publisher batching (`batching.max_messages = 1000`, `batching.max_delay = 50ms`) before calling `pubsub.publish()`.

#### Pattern B: Pull-Based Polling (Legacy & Air-Gapped Systems)
Used for legacy on-premise monitoring databases (e.g., Oracle/SQL Server operational tables, custom monitoring daemons) unable to push outbound webhooks.
* **Transport**: Private connectivity via **Dedicated Cloud Interconnect** or **HA Cloud VPN**.
* **Orchestration**: **Cloud Scheduler** triggers a containerized **Cloud Run Job** on a 1-minute cron schedule.
* **Pagination & Watermarking**: The poller maintains a high-watermark timestamp in Cloud Storage/Firestore, extracts only delta records, transforms them into JSON, and publishes them into `telemetry.legacy.raw`.

#### Pattern C: Direct Cloud-Native Routing (GCP Operations Suite)
Used for GKE cluster logs, Cloud Audit Logs, VPC Flow Logs, and Cloud Monitoring metrics.
* **Transport**: GCP internal backbone using **Cloud Logging Log Router**.
* **Zero-Compute Ingress**: Ingestion filters route events directly into `telemetry.gcp.raw` without intermediate compute or proxy layers.

---

## 2. Decoupled Buffer Layer: Cloud Pub/Sub Fleet

Cloud Pub/Sub provides horizontal scalability, zero-maintenance partition management, and strict isolation between monitoring domains.

### 2.1 Topic Topology & Configuration

| Topic Identifier | Target Source | Ingestion Format | Daily Volume | Peak Throughput (EPS) | Retention | Dead-Letter Topic |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `telemetry.akamai.raw` | Akamai DataStream 2 | JSON / Gzip | 4 – 6 TB | 150,000 EPS | 7 days | `telemetry.akamai.dlq` |
| `telemetry.dynatrace.raw` | Dynatrace Davis AI & Spans | JSON (Webhook) / OTel | 2 – 4 TB | 80,000 EPS | 7 days | `telemetry.dynatrace.dlq` |
| `telemetry.gcp.raw` | GCP Ops, GKE & Audit | Cloud Logging Proto / JSON | 3 – 5 TB | 100,000 EPS | 7 days | `telemetry.gcp.dlq` |
| `telemetry.splunk.raw` | Splunk Forwarder / SIEM | JSON / CEF | 3 – 6 TB | 90,000 EPS | 7 days | `telemetry.splunk.dlq` |
| `telemetry.adobe.raw` | AEP Streaming Clickstream | JSON | 1 – 2 TB | 40,000 EPS | 7 days | `telemetry.adobe.dlq` |
| `telemetry.legacy.raw` | On-Prem Polled Databases | Structured JSON | 500 GB | 10,000 EPS | 7 days | `telemetry.legacy.dlq` |
| `aiops.alerts.actionable` | Canonical Anomaly Stream | Avro / JSON | 50 GB | 2,000 EPS | 14 days | `aiops.alerts.dlq` |

### 2.2 Poison Message Isolation & DLQ Strategy
To prevent malformed payloads ("poison pills") from causing infinite retry loops and stalling Dataflow workers:
* **Ack Deadline**: Set to `60 seconds` (accommodates batching and DLP scrubbing).
* **Max Delivery Attempts**: `5`.
* **Dead-Letter Redirection**: If a message fails schema parsing or crashes a worker 5 times consecutively, Pub/Sub automatically forwards it to `telemetry.<source>.dlq` with custom delivery attempt metadata and publishes an alert to Cloud Monitoring.

---

## 3. Dataflow Streaming Pipeline: Deep Processing Topology

The streaming processing tier runs an autoscaling **Apache Beam** pipeline on **Google Cloud Dataflow (Streaming Engine)** implementing a unified 6-stage transformation graph.

```mermaid
flowchart TD
    %% DATAFLOW INTERNAL STAGES
    subgraph Beam_Graph["Apache Beam 6-Stage Execution Graph"]
        direction TB
        S1["<b>Stage 1: Multi-Topic Ingest</b><br/>PubsubIO.readMessagesWithAttributes()"]
        S2["<b>Stage 2: Schema Validation</b><br/>JSON Schema vs. Side-Output DLQ"]
        S3["<b>Stage 3: Stateful Deduplication</b><br/>10-min Sliding Window & Hash State"]
        S4["<b>Stage 4: Hybrid DLP Engine</b><br/>Regex Fast-Path + Cloud DLP API"]
        S5["<b>Stage 5: Canonical Normalization</b><br/>Mapping to AIOpsCanonicalEvent"]
        S6["<b>Stage 6: Windowing & Egress</b><br/>Event-Time Windows + Storage Write API"]
    end

    DLQ_Sink["🚨 <b>Side-Output: Dead-Letter Queue</b><br/>telemetry.dlq"]

    S1 --> S2
    S2 -->|"Valid Records"| S3
    S2 -.->|"Malformed Payload"| DLQ_Sink
    S3 --> S4
    S4 --> S5
    S5 --> S6
```

### 3.1 Detailed Pipeline Stages

#### Stage 1: High-Throughput Ingestion (`PubsubIO`)
* Reads concurrently from all 6 raw Pub/Sub subscriptions using `PubsubIO.readMessagesWithAttributes()`.
* Retains transport-level attributes: `X-Akamai-Request-ID`, `dynatrace-event-token`, client IP, and ingress timestamps.

#### Stage 2: Schema Validation & DLQ Side-Output
* Parses incoming payloads against standard schema definitions.
* Valid records pass to Stage 3. Unparseable JSON or payloads violating mandatory field typing (`timestamp`, `source_tool`) are branched to a **Side-Output PCollection** and written directly to the Dead-Letter Queue without halting pipeline execution.

#### Stage 3: Stateful Event Deduplication
Due to upstream HTTP retries from Akamai or Splunk forwarders, duplicate records are common.
* **Stateful DoFn**: Implements `@StateId("seen_hashes") ValueState<Boolean>` with an associated `@TimerId("gc_timer")`.
* **Deduplication Key**: Computed SHA-256 hash of `[source_tool + original_event_id + event_timestamp]`.
* **Sliding Buffer**: Holds state for a 10-minute rolling buffer before garbage collecting expired keys.

#### Stage 4: High-Throughput Hybrid Cloud DLP Engine
Processing 460,000 EPS through the external Cloud DLP API directly is cost-prohibitive and quota-limiting. We employ a **2-tier Hybrid DLP Architecture**:
1. **Tier 1 (In-Worker Regex Fast-Path)**: Apache Beam worker memory executes compiled regex matching with the **Luhn Algorithm** to instantly mask credit card numbers, CVVs, and standard SSN patterns directly on the worker CPU ($0$ additional network latency, $0$ API cost).
2. **Tier 2 (Scoped Cloud DLP API Batching)**: Only unstructured free-form text fields (`raw_log`, `stack_trace`, `user_agent_payload`) are batched into groups of 500 and dispatched asynchronously over gRPC channels to the **Cloud DLP API** for advanced dictionary and context-aware PII scrubbing.

#### Stage 5: Canonical Normalization
Transforms heterogeneous source formats into the standardized `AIOpsCanonicalEvent` Avro contract:
* Maps `Dynatrace Davis Problem` $\rightarrow$ Standard Anomaly entity with affected topology nodes.
* Maps `Akamai Origin 504 surges` $\rightarrow$ Standard Network/Edge degradation entity.
* Maps `Adobe OPM Drop` $\rightarrow$ Standard Business KPI Impact entity.
* **Alert Branching**: High-severity anomalies (`severity IN ('CRITICAL', 'FATAL')`) are cloned and published immediately to `aiops.alerts.actionable` for consumption by Vertex AI and automated ServiceNow incident creation.

#### Stage 6: Event-Time Windowing & BigQuery Storage Write API
* **Event-Time Watermarks**: Pipeline windows events using the source's native `event_timestamp`, rather than the GCP ingestion timestamp.
* **Allowed Lateness**: Configured with a `15-minute` allowed lateness window. Events arriving later than 15 minutes are branched to the `Late_Data_Sink` in GCS to prevent corrupting real-time analytical baseline calculations.
* **BigQuery Storage Write API**: Writes canonical events via `BigQueryIO.write().withMethod(Method.STORAGE_WRITE_API)` ensuring exactly-once streaming insertion, sub-second query availability, and zero data duplication.

---

## 4. Downstream Storage Optimization (BigQuery & GCS)

To support both instant AI root-cause analysis and cost-effective long-term ML training:

### 4.1 BigQuery Lakehouse Design
* **Table Partitioning**: Partitioned by day on `event_timestamp` (`_PARTITIONDATE`), with a 90-day hot partition expiration.
* **Table Clustering**: Multi-column clustered on `source_tool`, `severity`, and `service_name`.
* **Query Performance**: Clustering allows Vertex AI agents and SRE dashboards to filter across 50 TB of logs in $< 800\text{ ms}$ while scanning $< 100\text{ MB}$ of data.

### 4.2 GCS Cold Storage Parquet Archiving
* In parallel with BigQuery writes, Dataflow aggregates clean records into 1-hour tumbling windows.
* Flushes data to GCS using `FileIO.write()` configured with **Snappy-compressed Apache Parquet format**.
* Organized in Hive-style folder structures: `gs://aiops-telemetry-lake/raw/source_tool=akamai/year=2026/month=08/day=26/`.

---

## 5. Ingestion Sizing, Reliability & Observability SLAs

### 5.1 Elastic Compute Sizing
* **Worker Fleet**: `n2-standard-4` (4 vCPU, 16 GB RAM) autoscaling dynamically from **10 workers** (normal load) up to **100 workers** (promotional peak).
* **Streaming Engine**: Managed GCP Streaming Engine enabled to offload window state storage from worker memory.
* **Backpressure Management**: Flow control limits maximum active Pub/Sub reads based on downstream BigQuery commit latency.

### 5.2 SRE Observability & Alerting Policies
The ingestion infrastructure is self-monitoring using GCP Cloud Monitoring metrics linked to ServiceNow:

```mermaid
flowchart TD
    subgraph Health_Metrics["Ingestion Pipeline Health Metrics"]
        direction TB
        M1["⏱️ <b>Dataflow System Lag</b><br/><code>dataflow/system_lag > 30s</code>"]
        M2["📬 <b>Pub/Sub Backlog Age</b><br/><code>pubsub/oldest_unacked_age > 60s</code>"]
        M3["🚨 <b>Poison Message Surge</b><br/><code>DLQ Message Rate > 10/min</code>"]
    end

    AlertRouter["🔔 <b>Cloud Monitoring Alert Policy</b><br/>Multi-condition incident aggregation"]
    SNOW["🎫 <b>ServiceNow P2 Incident</b><br/>Dispatched to Data Engineering On-Call"]

    M1 --> AlertRouter
    M2 --> AlertRouter
    M3 --> AlertRouter
    AlertRouter --> SNOW
```

1. **Pipeline Processing Lag**: Alert triggered if `dataflow.googleapis.com/job/system_lag` exceeds `30 seconds`.
2. **Shock Absorber Backlog**: Alert triggered if `pubsub.googleapis.com/subscription/oldest_unacked_message_age` exceeds `60 seconds`.
3. **Dead-Letter Surge**: Alert triggered if any DLQ topic receives $> 10\text{ messages/minute}$, immediately dispatching a high-priority incident to the Data Engineering on-call queue in **ServiceNow**.
