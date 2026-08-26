# 01. Ingestion Layer

Welcome to the **Ingestion Layer** documentation. This module covers the end-to-end telemetry capture, buffering, scrubbing, and streaming pipelines from heterogeneous multi-cloud and on-premise observability systems into Google Cloud Platform.

---

## 📚 Core Architecture Documents

1. ⚡ **[Ingestion Architecture & Streaming Pipelines (Master Guide)](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/ingestion_architecture.md)**
   - *First-Mile source extraction, Cloud Armor WAF & Cloud Run Ingress Gateway, Pub/Sub shock absorber fleet, 6-stage Dataflow (Beam) streaming engine, 2-tier Hybrid DLP, BigQuery/GCS partitioning, and automated ServiceNow alerting.*

2. 📜 **[Data Contracts & Canonical Schemas](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/data_contracts_and_schemas.md)**
   - *Common Event Format (CEF) / Avro canonical contracts, field mapping dictionaries, and Cloud DLP redaction policies.*

---

## 🌐 Source-Specific Connector Specifications

Explore the dedicated ingestion guides for the SRE team's 5 primary observability tools:

* 🌐 **[Akamai DataStream Connector](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/connectors/akamai_datastream.md)**: Edge access logs, TTFB metrics, and WAF security triggers.
* ⚡ **[Dynatrace Ingestion Connector](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/connectors/dynatrace_ingestion.md)**: PurePath spans, Smartscape topology, and Davis AI problem webhooks.
* ☁️ **[GCP Operations Ingestion Connector](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/connectors/gcp_ops_ingestion.md)**: GKE cluster logs, Cloud Audit Logs, and Managed Prometheus metric scraping.
* 📜 **[Splunk HEC Connector](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/connectors/splunk_hec_ingestion.md)**: HTTP Event Collector forwarding, SIEM alerts, and forensic search proxying.
* 🛍️ **[Adobe Analytics Streaming Connector](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/connectors/adobe_analytics_stream.md)**: Real-time clickstream events and Orders Per Minute (OPM) drop detection.
