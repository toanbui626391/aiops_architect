# AIOps Ingestion Pipeline: Best Practices

This guide outlines the best practices for designing, configuring, and operating the AIOps ingestion pipelines based on the core architecture defined in [ingestion_architecture.md](ingestion_architecture.md). Adhering to these guidelines ensures scalability, security, and high reliability across multi-cloud observability sources.

---

## 1. Design Best Practices

### 1.1 Connector Pattern Selection
Always select the ingestion connector pattern based on the source's capabilities and network location:
*   **Use Native Connectors (Cloud Logging/Log Router Sinks)** for any workloads already running on Google Cloud (e.g., GKE, Dataflow). This guarantees sub-second latency and zero egress costs.
*   **Use Push Connectors (Cloud Run)** for modern SaaS platforms (Akamai, Dynatrace, Adobe, Splunk HEC) that support real-time webhooks or streaming exports.
*   **Use Pull Connectors (Cloud Composer / Apache Airflow)** *only* for legacy on-premise databases or air-gapped systems that cannot push data out. Avoid using Cloud Run jobs for Pull scenarios, as Airflow provides superior retry logic and operational visibility for complex extraction tasks.

### 1.2 Strict Decoupling
*   **Never connect sources directly to processing engines.** All incoming telemetry must land in **Cloud Pub/Sub** first.
*   Use isolated, dedicated Pub/Sub topics for each major data source (e.g., `telemetry.akamai.raw`, `telemetry.splunk.raw`). This prevents a traffic flood from one source (like a DDoS attack logged by Akamai) from starving resources for other critical telemetry.

---

## 2. Configuration Best Practices

### 2.1 Security & Authentication
*   **Never hardcode credentials.** Cloud Run push connectors and Cloud Composer pull tasks must retrieve API keys, HMAC secrets, and database credentials dynamically from **GCP Secret Manager** at runtime.
*   Protect all public-facing Cloud Run push connectors with **Cloud Armor WAF**. Enforce strict IP allowlisting (e.g., only accepting traffic from known Akamai or Dynatrace CIDR blocks) and implement rate-limiting to prevent malicious flooding.

### 2.2 Schema Management
*   Enable and enforce the **Pub/Sub Schema Registry** using Protobuf or Avro.
*   Connectors must validate JSON payloads against the registry before publishing. This prevents upstream SaaS providers from pushing undocumented, breaking schema changes that crash downstream Dataflow pipelines.

### 2.3 Resiliency Configuration
*   **Multi-Region Pub/Sub**: Configure critical telemetry topics for cross-region message routing so that data ingestion survives single-zone or single-region outages.
*   **Dataflow Auto-scaling**: Configure Dataflow streaming jobs with a generous `maxNumWorkers` limit. This allows the pipeline to gracefully absorb sudden spikes in error logs during a massive P1 outage.

---

## 3. Operational & Observability Best Practices

### 3.1 Dead-Letter Queues (DLQs)
*   **Never silently drop data.**
*   Configure Pub/Sub native DLQs (after 5 failed delivery attempts) to catch messages that repeatedly crash Dataflow workers.
*   Configure Dataflow side-outputs to catch messages that parse successfully but fail business logic validation (e.g., missing timestamps).
*   Route all DLQ streams to a dedicated **Cloud Storage (GCS) Bucket** partitioned by date (`gs://aiops-dlq-bucket/YYYY/MM/DD/`) so data engineers can inspect and replay the data later.

### 3.2 Key Service Level Indicators (SLIs) to Monitor
You must configure **Cloud Monitoring** dashboards and alerts for the following critical ingestion SLIs:
*   **Pub/Sub `oldest_unacked_message_age`**: If this exceeds 5 minutes, it indicates the Dataflow pipeline is stalled or falling behind.
*   **Dataflow `system_lag` and `data_watermark_age`**: Monitors the real-time processing delay.
*   **DLQ Bucket Depth**: A sudden spike in DLQ volume indicates an upstream schema change or a bad code deployment in the Dataflow parsing logic.

### 3.3 Incident Alerting Routing
*   Route all critical ingestion pipeline alerts directly to **ServiceNow** via Cloud Monitoring webhooks. 
*   If the AIOps platform itself is failing to ingest data, the internal Data Platform SRE team must be paged immediately, as "silent monitoring failures" are the highest risk to enterprise uptime.
