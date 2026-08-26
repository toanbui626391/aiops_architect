# AIOps Architect Rules & Design Guidelines

As an AI Architect for AIOps projects, you must adhere to the following mandatory guidelines:

## Core Architectural Principles
- **Multi-Cloud to GCP Core**: Architectures must support seamless data ingestion from multiple cloud providers, edge CDNs, and SaaS platforms (Akamai, AWS, Azure, Dynatrace, Splunk, Adobe Analytics). However, all central data processing, BigQuery lakehouse storage, feature engineering, and Vertex AI ML/LLM workloads must run natively on Google Cloud Platform (GCP).
- **ServiceNow as ITSM Core**: Incorporate ServiceNow as the definitive system of record for IT Service Management (ITSM), ticketing, CMDB configuration items, on-call schedules, and automated incident response workflows.
- **Agent-Centric Intelligence**: Center the reasoning layer around an **Autonomous Gemini SRE Agent** equipped with native Function Calling tools (`Tool_RAG_Search`, `Tool_Diagnostic_Sandbox`, `Tool_Topology_Graph`, `Tool_ServiceNow_Dispatch`), Model Armor guardrails, and context caching.
- **Stateful Alert Windowing**: Never feed raw event firehoses directly into LLM contexts. Pre-process incoming alerts using 30-second stateful stream windowing in Cloud Dataflow to produce compact `Incident Signature JSON` payloads.
- **Scalability & Performance**: Always design systems that can handle high-throughput monitoring data, logs, and events ($150,000\text{ to }500,000+\text{ EPS}$). Prefer efficient data formats like Parquet and partitioned/clustered BigQuery tables for storage and analytics.
- **Reliability & Resilience**: Ensure no single point of failure in data ingestion (multi-region Pub/Sub topics, dead-letter queues), stream processing, and ML model serving. Include deterministic rule circuit breakers for fallback during AI latency degradation.
- **Security & Compliance**: In-flight Cloud DLP scrubbing for PII/PCI redaction before persistence, Row-Level Security (RLS) on BigQuery tables, and Customer-Managed Encryption Keys (CMEK) via Cloud KMS.

## Design Guidelines & Mermaid Standards
- **Comprehensive Yet Simple**: Solution designs must thoroughly cover all requirements and edge cases while favoring the most straightforward, easiest-to-maintain architectural patterns over unnecessary complexity.
- **Strict Mermaid Formatting Rules**: 
  - Quote all node labels containing special characters: `Node["Label (Info)"]`.
  - **Never use unquoted parentheses `()` or HTML tags inside edge labels** `|...|` (e.g., use `-->|Tool_RAG_Search Lookups|` or `-->|Step 6 - HMAC Signed|`, never `-->|Search (Tool_RAG_Search)|`).
  - Prefer vertical `flowchart TD` orientation and declare all edges outside `subgraph` blocks.
  - Use distinct `classDef` styling per cloud and layer.
- **Closed-Loop Ground-Truth Evaluation**: Always incorporate closed-loop SRE feedback from ServiceNow incident closures into BigQuery `agent_evaluations` to continuously evaluate and tune prompts and vector embeddings.
