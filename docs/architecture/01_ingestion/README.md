# 01. Ingestion Layer

Welcome to the **Ingestion Layer** documentation. This module covers the end-to-end telemetry capture, buffering, scrubbing, and streaming pipelines from heterogeneous multi-cloud and on-premise observability systems into Google Cloud Platform.

---

## 📚 Core Architecture Documents

1. ⚡ **[Ingestion Architecture & Streaming Pipelines (Master Guide)](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/ingestion_architecture.md)**
   - *First-Mile source extraction, Cloud Armor WAF & Cloud Run Ingress Gateway, Pub/Sub shock absorber fleet, Dataflow (Beam) streaming engine, Hybrid DLP, and BigQuery lakehouse streaming.*

2. 📊 **[SRE Observability Fleet - Source Telemetry Matrix](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/source_telemetry_matrix.md)**
   - *Unified telemetry profiles and canonical field mappings for Akamai, Dynatrace, GCP Ops, Splunk, and Adobe Analytics.*

3. 📜 **[Data Contracts & Canonical Schemas](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/data_contracts_and_schemas.md)**
   - *Canonical schemas, OpenTelemetry mappings, and Cloud DLP PII/PCI scrubbing rules.*

4. 💡 **[Ingestion Best Practices Guide](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/ingestion_best_practices.md)**
   - *Ingestion SLIs, pipeline watermarks, sliding deduplication, and dead-letter queue recovery.*
