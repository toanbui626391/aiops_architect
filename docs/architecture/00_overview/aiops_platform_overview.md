# Enterprise AIOps Platform - High-Level Architecture Overview

## 1. Executive Summary & Business Objective

Modern global enterprise operations face extreme telemetry fragmentation. For a large multi-channel retail organization, operations span thousands of edge locations, extensive cloud footprints, and millions of customer transactions per hour.

The **Enterprise AIOps Platform** consolidates disparate observability streams from the SRE team's 5 core tools—**Google Cloud Platform (GCP)**, **Dynatrace**, **Akamai**, **Splunk**, and **Adobe Analytics**—into a centralized intelligence engine natively hosted on Google Cloud Platform. The platform correlates business transactions with technical performance to automate incident triage, accelerate root cause identification, and trigger proactive remediation in **ServiceNow**.

```mermaid
flowchart LR
    subgraph Sources["Observability Telemetry"]
        direction TB
        S1["🌐 Akamai"]
        S2["🛍️ Adobe Analytics"]
        S3["⚡ Dynatrace"]
        S4["☁️ GCP Operations"]
        S5["📜 Splunk"]
    end

    subgraph GCP["GCP AIOps Core"]
        direction TB
        Ingest["High-Throughput Ingestion<br/>(Pub/Sub + Dataflow)"]
        Lakehouse["Unified Data Lakehouse<br/>(BigQuery + Parquet)"]
        AI["Reasoning & Intelligence<br/>(Vertex AI + Gemini)"]
    end

    subgraph Action["Enterprise Action"]
        direction TB
        SNOW["ServiceNow ITSM"]
        SRE["SRE Remediation"]
    end

    Sources ==> Ingest
    Ingest --> Lakehouse
    Ingest --> AI
    Lakehouse <--> AI
    AI ==> SNOW
    SNOW --> SRE

    classDef s fill:#ECEFF1,stroke:#37474F,stroke-width:2px,color:#263238;
    classDef g fill:#EDE7F6,stroke:#512DA8,stroke-width:2px,color:#311B92;
    classDef a fill:#E0F2F1,stroke:#00695C,stroke-width:2px,color:#004D40;

    class S1,S2,S3,S4,S5 s;
    class Ingest,Lakehouse,AI g;
    class SNOW,SRE a;
```

---

## 2. Core Architectural Pillars

### 2.1 Multi-Source Ingestion to GCP Core
While telemetry originates from diverse specialized tools across edge networks, cloud platforms, and enterprise data centers, **all centralized stream processing, analytical storage, and AI reasoning workloads run natively on Google Cloud Platform (GCP)**. This prevents vendor lock-in across tool providers while consolidating data for cross-domain ML models.

### 2.2 ServiceNow as the Definitive ITSM System of Record
**ServiceNow** is the enterprise system of record for all incident lifecycles, configuration item (CMDB) topologies, on-call schedules, and automated remediation audit logs. The AIOps layer communicates bidirectionally via standard ServiceNow REST APIs to enrich existing records, generate consolidated incidents, and bypass manual L1 triage.

### 2.3 Event-Driven Streaming & Elastic Scale
The platform is built on an event-driven architecture using **Cloud Pub/Sub** and **Cloud Dataflow (Apache Beam)**. It comfortably sustains normal loads of 150,000+ Events Per Second (EPS) and auto-scales to absorb peak promotional traffic (e.g., Black Friday / Cyber Monday surges exceeding 500,000 EPS) without message loss or latency degradation.

### 2.4 Actionable AI & Closed-Loop Diagnostics
Rather than producing passive dashboards, the AI architecture utilizes **Large Language Models (Gemini on Vertex AI)** and **BigQuery ML** to produce tangible operational actions:
* De-duplicating cascading alerts into single root-cause incidents.
* Retrieving matching Standard Operating Procedures (SOPs) from **Vertex AI Vector Search**.
* Automatically executing non-destructive diagnostic queries and posting synthesized findings to ServiceNow tickets.

---

## 3. End-to-End Architectural Flow

```mermaid
flowchart TD
    %% 1. SOURCES
    subgraph Sources["1. SRE Observability Fleet"]
        direction LR
        Akamai["🌐 <b>Akamai</b><br/>• Edge Latency & TTFB<br/>• WAF / DDoS Alerts<br/>• Bot Traffic Scoring"]
        Adobe["🛍️ <b>Adobe Analytics</b><br/>• Orders / Min (OPM)<br/>• Cart Abandonment Rate<br/>• Checkout Funnel Drops"]
        Dyna["⚡ <b>Dynatrace</b><br/>• PurePath Traces<br/>• Smartscape Topology<br/>• Davis AI RCA Webhooks"]
        GCPOps["☁️ <b>GCP Ops Suite</b><br/>• GKE Pod Health<br/>• Pub/Sub & Dataflow Lag<br/>• Cloud Audit Logs"]
        Splunk["📜 <b>Splunk</b><br/>• Enterprise Server Logs<br/>• POS & Middleware Logs<br/>• SIEM Security Events"]
    end

    %% 2. INGESTION & DATA LAKEHOUSE (GCP)
    subgraph GCP_Data["2. GCP Ingestion & Storage Core"]
        direction TB
        PubSub["📬 <b>Cloud Pub/Sub</b><br/>High-throughput global event ingestion bus"]
        Dataflow["⚙️ <b>Cloud Dataflow</b><br/>Stream deduplication, schema normalization & DLP filtering"]
        BQ[("🗄️ <b>BigQuery Lakehouse</b><br/>Unified historical telemetry & baseline tables (Parquet)")]
        VectorDB[("🔍 <b>Vertex AI Vector Search</b><br/>SOP Runbook embeddings & past incident catalog")]
    end

    %% 3. AI INTELLIGENCE & REASONING LAYER (GCP)
    subgraph GCP_AI["3. AIOps Intelligence & Reasoning Layer (Vertex AI)"]
        direction TB
        BQML["📈 <b>Business Anomaly Engine (BQML)</b><br/>ARIMA_PLUS time-series models for silent outage detection"]
        SemanticRouter["🧠 <b>Semantic Intelligence Router (LLM)</b><br/>Payload parsing, alert correlation & noise reduction"]
        ContextAgent["🤖 <b>Context-Aware SRE Agent (Gemini)</b><br/>Multi-source RCA, Runbook retrieval & diagnostic execution"]
        SOPEngine["⚡ <b>Automated SOP Execution Engine</b><br/>Read-only diagnostic executor & remediation coordinator"]
    end

    %% 4. ENTERPRISE ACTION & ITSM
    subgraph Action_Layer["4. Enterprise Action Layer (ServiceNow)"]
        direction TB
        SNOW["🎫 <b>ServiceNow Incident Management</b><br/>• Direct SRE pod routing (bypassing L1)<br/>• Enriched with Root Cause & Splunk logs<br/>• Automated diagnostic findings attached"]
        SREPods["👨‍💻 <b>On-Call SRE Pods</b><br/>One-click runbook remediation & accelerated MTTR"]
    end

    %% 5. GITOPS & CI/CD
    subgraph GitOps["5. CI/CD & GitOps"]
        direction TB
        GHA["🐙 <b>GitHub Actions</b><br/>Alerts as Code & SOP sync pipelines"]
    end

    %% INTER-LAYER DATA PIPELINE CONNECTIONS
    Akamai -->|DataStream 2 Push| PubSub
    Adobe -->|Streaming Clickstream / AEP| PubSub
    Dyna -->|Problem Webhooks & OTel| PubSub
    GCPOps -->|Log Sinks & GMP Scraper| PubSub
    Splunk -->|HEC Event Forwarding| PubSub

    PubSub --> Dataflow
    Dataflow -->|Stream Clean Telemetry| BQ
    Dataflow -->|Alert Events| SemanticRouter

    BQ -->|Continuous Baseline Evaluation| BQML
    BQML -->|Trigger Business Alert| SemanticRouter

    SemanticRouter -->|Correlated Alert Context| ContextAgent
    VectorDB <-->|Query Relevant Runbook / SOP| ContextAgent
    ContextAgent --> SOPEngine

    SemanticRouter -->|Intelligent Ticket Creation & Pod Routing| SNOW
    ContextAgent -->|Contextual Updates, RCA & Splunk Links| SNOW
    SOPEngine <-->|Execute Diagnostics & Attach Output| SNOW
    SNOW -->|Page On-Call Engineer| SREPods

    GHA -.->|Deploy Monitoring Rules & Thresholds| Sources
    GHA -.->|Sync SOP Markdown to Vector Index| VectorDB

    %% STYLING DIRECTIVES
    classDef akamaiStyle fill:#FFF3E0,stroke:#E65100,stroke-width:2px,color:#BF360C;
    classDef adobeStyle fill:#FFEBEE,stroke:#C62828,stroke-width:2px,color:#B71C1C;
    classDef dynaStyle fill:#E3F2FD,stroke:#1565C0,stroke-width:2px,color:#0D47A1;
    classDef gcpOpsStyle fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px,color:#1B5E20;
    classDef splunkStyle fill:#ECEFF1,stroke:#37474F,stroke-width:2px,color:#263238;
    classDef gcpCoreStyle fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px,color:#4A148C;
    classDef gcpAIStyle fill:#EDE7F6,stroke:#303F9F,stroke-width:2px,color:#1A237E;
    classDef snowStyle fill:#E0F2F1,stroke:#00796B,stroke-width:2px,color:#004D40;
    classDef gitopsStyle fill:#ECEFF1,stroke:#24292E,stroke-width:2px,color:#24292E;

    class Akamai akamaiStyle;
    class Adobe adobeStyle;
    class Dyna dynaStyle;
    class GCPOps gcpOpsStyle;
    class Splunk splunkStyle;
    class PubSub,Dataflow,BQ,VectorDB gcpCoreStyle;
    class BQML,SemanticRouter,ContextAgent,SOPEngine gcpAIStyle;
    class SNOW,SREPods snowStyle;
    class GHA gitopsStyle;
```

---

## 4. Architectural Domain Map & Detailed References

| Architecture Domain | Scope & Responsibilities | Deep-Dive Specification |
| :--- | :--- | :--- |
| **01. Ingestion Architecture** | Transport protocols, Pub/Sub topologies, Dataflow Beam pipelines, Cloud DLP scrubbing, dead-letter queues, and connector configs for all 5 tools. | [01_ingestion/README.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/README.md) |
| **02. Storage & Lakehouse** | BigQuery dataset designs, table partitioning by ingestion time, Parquet on GCS cold storage tiering, Iceberg metadata, and query optimization. | [lakehouse_architecture.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/02_storage_and_lakehouse/lakehouse_architecture.md) |
| **03. Intelligence & Reasoning** | Vertex AI LLM routing models, Gemini SRE Agent, Vertex Vector Search for SOPs, and BigQuery ML `ARIMA_PLUS` models for silent outage detection. | [aiops_intelligence_layer.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/03_intelligence_and_reasoning/aiops_intelligence_layer.md) |
| **04. Observability Ecosystem** | Comprehensive guide on Akamai, Dynatrace, GCP Ops, Splunk, and Adobe Analytics—telemetry formats, metric semantics, and AI use cases. | [observability_tools_guide.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/04_observability_ecosystem/observability_tools_guide.md) |
| **05. ITSM & Remediation** | ServiceNow Incident Management integration, bidirectional lifecycle synchronization, CMDB topological sync, and automated diagnostic execution. | [servicenow_integration.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/05_itsm_and_remediation/servicenow_integration.md) |
