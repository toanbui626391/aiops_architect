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
        Lakehouse["Unified Data Lakehouse & Features<br/>(BigQuery + Feature Store)"]
        AI["Autonomous Gemini SRE Agent<br/>(Supportive Tools & RAG)"]
    end

    subgraph Action["Enterprise Action"]
        direction TB
        SNOW["ServiceNow ITSM"]
        SRE["SRE Remediation"]
    end

    Sources ==> Ingest
    Ingest --> Lakehouse
    Lakehouse --> AI
    AI ==> SNOW
    SNOW --> SRE
    SNOW -.->|Closed-Loop Feedback| Lakehouse

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

### 2.4 Actionable AI & Autonomous Incident Diagnostics
Rather than producing passive dashboards, the AI architecture utilizes the **Autonomous Gemini SRE Agent** with **Supportive Function Calling Tools** to execute tangible operational workflows:
* De-duplicating cascading alerts into unified incident signatures.
* Retrieving matching Standard Operating Procedures (SOPs) from **Vertex AI Vector Search**.
* Automatically executing non-destructive diagnostic queries and posting synthesized findings to ServiceNow tickets.
* Capturing closed-loop SRE feedback upon ticket closure to continuously evaluate and tune prompts.

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

    %% 2. INGESTION, STORAGE & FEATURE PREPARATION (GCP)
    subgraph GCP_Data["2. GCP Ingestion, Lakehouse & Feature Preparation"]
        direction TB
        PubSub["📬 <b>Cloud Pub/Sub</b><br/>High-throughput global event ingestion bus"]
        Dataflow["⚙️ <b>Cloud Dataflow</b><br/>Alert storm windowing, OTel normalization & DLP filtering"]
        BQ[("🗄️ <b>BigQuery Lakehouse & Feature Store</b><br/>Canonical telemetry, OPM materialized views & topology graphs")]
        VectorDB[("🔍 <b>Vertex AI Vector Search</b><br/>SOP Runbook embeddings & past incident catalog")]
    end

    %% 3. AUTONOMOUS AGENT REASONING CORE (GCP)
    subgraph GCP_AI["3. Autonomous Gemini SRE Agent Core (Vertex AI)"]
        direction TB
        AgentCore["🤖 <b>Autonomous Gemini SRE Agent</b><br/>4-Phase Reasoning: Triage ➔ RCA ➔ Diagnostics ➔ ITSM Dispatch"]
        
        subgraph ToolSuite["Supportive Agentic Tool Suite"]
            direction LR
            RAGTool["🔍 <b>RAG Search Tool</b>"]
            SandboxTool["⚡ <b>AST Sandbox Tool</b>"]
            GraphTool["🕸️ <b>Topology Graph Tool</b>"]
        end

        Guardrails["🛡️ <b>Model Armor & Context Caching</b><br/>Prompt injection defense, secret scrubbing & schema caching"]
    end

    %% 4. ENTERPRISE ACTION & ITSM
    subgraph Action_Layer["4. Enterprise Action Layer (ServiceNow)"]
        direction TB
        DispatchQueue["📬 <b>Cloud Tasks Dispatcher</b><br/>Rate-limited API shock absorber (Max 20 req/s)"]
        SNOW["🎫 <b>ServiceNow Incident Management</b><br/>• Direct SRE pod routing (bypassing L1)<br/>• Enriched with Root Cause & Splunk logs<br/>• Automated diagnostic findings attached"]
        SREPods["👨‍💻 <b>On-Call SRE Pods</b><br/>One-click runbook remediation & accelerated MTTR"]
        EvalStore[("📊 <b>BigQuery Evaluation Store</b><br/>Closed-loop SRE feedback & accuracy tracking")]
    end

    %% INTER-LAYER DATA PIPELINE CONNECTIONS
    Akamai & Adobe & Dyna & GCPOps & Splunk --> PubSub
    PubSub --> Dataflow
    Dataflow -->|Stream Clean Telemetry| BQ

    Dataflow -->|Structured Incident Signature| AgentCore
    BQ -->|Time-Series Anomaly Triggers| AgentCore

    AgentCore <--> ToolSuite
    ToolSuite <-->|Vector Lookups| VectorDB
    ToolSuite <-->|AST Safe Queries & Graph Traversal| BQ
    AgentCore <--> Guardrails

    AgentCore -->|Enqueue Ticket Payload| DispatchQueue
    DispatchQueue -->|HTTP POST with correlation_id| SNOW
    SNOW -->|Page On-Call Engineer| SREPods
    
    SNOW -.->|Incident Closure Feedback Webhook| EvalStore
    EvalStore -.->|Continuous Prompt & Embedding Tuning| AgentCore

    %% STYLING DIRECTIVES
    classDef akamaiStyle fill:#FFF3E0,stroke:#E65100,stroke-width:2px,color:#BF360C;
    classDef adobeStyle fill:#FFEBEE,stroke:#C62828,stroke-width:2px,color:#B71C1C;
    classDef dynaStyle fill:#E3F2FD,stroke:#1565C0,stroke-width:2px,color:#0D47A1;
    classDef gcpOpsStyle fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px,color:#1B5E20;
    classDef splunkStyle fill:#ECEFF1,stroke:#37474F,stroke-width:2px,color:#263238;
    classDef gcpCoreStyle fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px,color:#4A148C;
    classDef gcpAIStyle fill:#EDE7F6,stroke:#303F9F,stroke-width:2px,color:#1A237E;
    classDef snowStyle fill:#E0F2F1,stroke:#00796B,stroke-width:2px,color:#004D40;

    class Akamai akamaiStyle;
    class Adobe adobeStyle;
    class Dyna dynaStyle;
    class GCPOps gcpOpsStyle;
    class Splunk splunkStyle;
    class PubSub,Dataflow,BQ,VectorDB gcpCoreStyle;
    class AgentCore,ToolSuite,Guardrails gcpAIStyle;
    class DispatchQueue,SNOW,SREPods,EvalStore snowStyle;
```

---

## 4. Architectural Domain Map & Detailed References

| Architecture Domain | Scope & Responsibilities | Deep-Dive Specification |
| :--- | :--- | :--- |
| **01. Ingestion Architecture** | Transport protocols, Pub/Sub topologies, Dataflow Beam pipelines, Cloud DLP scrubbing, dead-letter queues, and unified telemetry matrix for all 5 observability tools. | • [01_ingestion/README.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/README.md)<br/>• [source_telemetry_matrix.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/source_telemetry_matrix.md) |
| **02. Storage, Lakehouse & Data Processing** | BigQuery dataset designs, table partitioning by ingestion time, Parquet on GCS cold storage tiering, stateful alert clustering, time-series resampling, and topology graph ETL. | • [lakehouse_architecture.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/02_storage_and_lakehouse/lakehouse_architecture.md)<br/>• [data_processing_and_feature_store.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/02_storage_and_lakehouse/data_processing_and_feature_store.md) |
| **03. Intelligence & Reasoning** | Autonomous Gemini SRE Agent runtime, 4-phase reasoning lifecycle, supportive function calling tools (RAG, AST Sandbox, Topology Graph), Model Armor guardrails, and context caching. | [aiops_intelligence_layer.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/03_intelligence_and_reasoning/aiops_intelligence_layer.md) |
| **04. ITSM & Remediation** | ServiceNow Incident Management integration, bidirectional lifecycle synchronization, CMDB topological sync, and automated diagnostic execution. | [servicenow_integration.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/04_itsm_and_remediation/servicenow_integration.md) |
