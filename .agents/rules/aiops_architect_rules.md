# AIOps Architect Rules & Design Guidelines

As an AI Architect designing, evaluating, or implementing architectures for the Enterprise AIOps Platform, you MUST adhere to the following mandatory rules without exception:

---

## 1. Core Architectural Mandates

1. **Multi-Cloud Ingestion to GCP Core**:
   * Telemetry may originate from diverse multi-cloud, edge, and SaaS environments (Akamai, AWS, Azure, Dynatrace, Splunk, Adobe Analytics, on-premise data centers).
   * **All central data processing, BigQuery lakehouse storage, feature engineering, and Vertex AI ML/LLM workloads must run natively on Google Cloud Platform (GCP).**
2. **ServiceNow as the ITSM System of Record**:
   * **ServiceNow** is strictly the system of record for Incident Management, CMDB configuration items, on-call team routing, and automated remediation audit trails.
   * Direct L1 manual triage must be bypassed via automated SRE pod routing based on CMDB mapping.
3. **Agent-Centric Intelligence & Supportive Tooling**:
   * Prioritize the **Autonomous Gemini SRE Agent** architecture executing structured reasoning lifecycles (Triage ➔ Multi-Source RCA ➔ Diagnostic Execution ➔ ServiceNow Action).
   * Frame supportive capabilities as native **Function Calling Tools** (`Tool_RAG_Search`, `Tool_Diagnostic_Sandbox`, `Tool_Topology_Graph`, `Tool_ServiceNow_Dispatch`).
4. **Stateful De-Noising & Token Protection**:
   * Never feed raw high-cardinality telemetry streams (150,000–500,000+ EPS) directly into LLM contexts.
   * Pre-process alerts using stateful stream windowing (Cloud Dataflow 30-second tumbling windows) to emit clean, unified `Incident Signature JSON` payloads to the agent.
5. **Security, Privacy & DLP at Ingestion**:
   * Ingested telemetry must pass through in-flight **Cloud DLP scrubbing** to redact PII/PCI (credit card numbers, auth tokens, passwords) before writing to BigQuery.
   * Enforce Row-Level Security (RLS) on BigQuery tables and Customer-Managed Encryption Keys (CMEK) via Cloud KMS.
6. **Closed-Loop Evaluation & Feedback**:
   * Every architecture proposal must include a closed-loop ground-truth evaluation mechanism (e.g., ServiceNow incident resolution webhook streaming into BigQuery `agent_evaluations`) to measure Routing Precision ($> 92\%$) and SOP Retrieval Recall ($> 88\%$).

---

## 2. Visual Documentation & Strict Mermaid Diagram Standards

All architecture designs, updates, or modifications MUST include clear Mermaid diagrams following these strict formatting constraints to prevent rendering and parsing errors:

1. **Edge Label Parentheses Prohibition (CRITICAL - No `PS` Syntax Errors)**:
   * **NEVER place parentheses `()` or HTML tags inside edge labels** `|...|`. Mermaid's lexer/parser treats `(` as the start of a node shape token (`PS` - Parenthesis Start), which breaks the entire diagram with `Parse error ... got 'PS'`:
     * ❌ *INCORRECT*: `A -->|Semantic Search (Tool_RAG_Search)| B`
     * ❌ *INCORRECT*: `A <-->|Dependency Traversal (Tool_Topology_Graph)| B`
     * ❌ *INCORRECT*: `A -.->|Execute Approved Action (HMAC Signed)| B`
     * ✅ *CORRECT*: `A -->|Tool_RAG_Search Semantic Lookups| B`
     * ✅ *CORRECT*: `A <-->|Tool_Topology_Graph Traversal| B`
     * ✅ *CORRECT*: `A -.->|Execute Approved Action - HMAC Signed| B`
2. **Node Label Quoting**:
   * **ALWAYS quote node labels** containing special characters like parentheses, brackets, colons, slashes, or HTML formatting tags:
     * ❌ *INCORRECT*: `NodeA[🌐 Akamai (Edge & Perimeter)]`
     * ❌ *INCORRECT*: `NodeB[🗄️ BigQuery: Canonical Table]`
     * ✅ *CORRECT*: `NodeA["🌐 <b>Akamai</b><br/>(Edge & Perimeter)"]`
     * ✅ *CORRECT*: `NodeB[("🗄️ <b>BigQuery</b><br/><code>aiops_lakehouse.telemetry_canonical</code>")]`
3. **Diagram Orientation & Subgraph Edge Declaration**:
   * Prefer vertical layouts (`flowchart TD` with subgraphs set to `direction TB` or `direction LR`) to ensure readability on standard screens.
   * **Declare all edge connections outside and after the `subgraph` blocks.** Never nest edge relationships inside subgraph containers.
4. **Color Palettes & Class Definitions**:
   * Apply distinct `classDef` styling to visually separate cloud tiers:
     * Sources / Edge: `#ECEFF1` / `#FFF3E0`
     * GCP Ingestion & Lakehouse: `#F3E5F5` (Purple)
     * Vertex AI & Agent Reasoning: `#EDE7F6` (Deep Purple / Indigo)
     * ServiceNow & Enterprise Action: `#E0F2F1` (Teal / Green)
5. **Pre-Flight Mermaid Verification**:
   * Always verify diagrams after modification by running the automated PDF export script:
     ```bash
     node scripts/export_docs_to_pdf.js
     ```
   * If any diagram contains a syntax error, Chrome/Mermaid will log a parse error during rendering. Ensure clean exit code 0.

---

## 3. Documentation Structure & Clean Repository Standards

When creating or modifying platform documentation, adhere to the 4-tier domain structure:
* `docs/architecture/00_overview/`: High-level platform blueprint, executive summary, end-to-end flows.
* `docs/architecture/01_ingestion/`: First-mile connectors, Pub/Sub topologies, canonical schemas, Cloud DLP, and the unified source telemetry matrix.
* `docs/architecture/02_storage_and_lakehouse/`: BigQuery canonical lakehouse, storage tiering, real-time feature views, topology graph ETL, and RAG vector storage.
* `docs/architecture/03_intelligence_and_reasoning/`: Autonomous Gemini SRE Agent runtime, reasoning lifecycle, supportive tools, guardrails, context caching, and evaluation store.
* `docs/architecture/04_itsm_and_remediation/`: ServiceNow Table API integration, Cloud Tasks rate-limiting buffer, `correlation_id` deduplication, CMDB sync, and HITL remediation.

---

## 4. Operational Metric Targets (SRE Best Practices)

* **Mean Time to Detect (MTTD)**: $< 60$ seconds via automated time-series anomaly triggers and Davis AI problem webhooks.
* **Triage & Routing Latency**: $< 2$ seconds from alert arrival to ServiceNow ticket dispatch.
* **SOP Retrieval Latency**: $< 100$ milliseconds via Vertex AI Vector Search ANN index.
* **Diagnostic Attachment**: Safe read-only telemetry and log evidence attached to the ticket prior to engineer arrival.
