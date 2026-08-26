# Enterprise AIOps Platform Architecture Documentation

Welcome to the architectural specification repository for the **Enterprise AIOps Platform**. This platform provides intelligent, automated incident response, predictive anomaly detection, and root cause analysis for the Site Reliability Engineering (SRE) team of a global retail organization.

---

## 🌳 Architectural Documentation Tree

```
docs/
├── README.md                                          # 📍 You are here (Master Index & Architecture Navigation)
└── architecture/
    ├── 00_overview/
    │   └── aiops_platform_overview.md                 # 🏛️ High-Level Platform Design, Business Context & End-to-End Flows
    ├── 01_ingestion/
    │   ├── README.md                                  # ⚡ Ingestion Module Overview & Directory Index
    │   ├── ingestion_architecture.md                  # ⚙️ Ingestion Master Blueprint: First-Mile & Streaming Pipelines
    │   ├── source_telemetry_matrix.md                 # 📊 SRE Observability Fleet: Profiles & Canonical Mappings (5 Tools)
    │   ├── data_contracts_and_schemas.md              # 📜 Canonical Event Schemas (CEF/JSON/Parquet) & Cloud DLP
    │   └── ingestion_best_practices.md                # 💡 Resilience, Deduplication, SLIs & Watermarking
    ├── 02_storage_and_lakehouse/
    │   ├── lakehouse_architecture.md                  # 🗄️ BigQuery Lakehouse, Parquet on GCS, Partitioning & Retention
    │   └── data_processing_and_feature_store.md       # ⚡ Stateful Alert Windowing, Feature Store, Graph ETL & RAG Vectorization
    ├── 03_intelligence_and_reasoning/
    │   └── aiops_intelligence_layer.md                # 🤖 Autonomous Gemini SRE Agent, Supportive Tooling & Guardrails
    └── 04_itsm_and_remediation/
        └── servicenow_integration.md                  # 🎫 ServiceNow ITSM Integration, CMDB Sync, Dispatch & HITL Remediation
```

---

## 🗺️ High-Level Platform Map

```mermaid
flowchart TD
    %% 1. SRE OBSERVABILITY FLEET
    subgraph Sources["1. SRE Observability Fleet"]
        direction LR
        Akamai["🌐 <b>Akamai</b><br/>Edge & Perimeter"]
        Adobe["🛍️ <b>Adobe Analytics</b><br/>Business Funnel"]
        Dyna["⚡ <b>Dynatrace</b><br/>APM & Trace Graph"]
        GCPOps["☁️ <b>GCP Ops Suite</b><br/>Cloud & GKE Infra"]
        Splunk["📜 <b>Splunk</b><br/>Logs & Forensics"]
    end

    %% 2. INGESTION & DATA LAKEHOUSE (GCP)
    subgraph GCP_Data["2. GCP Ingestion, Lakehouse & Feature Store"]
        direction TB
        PubSub["📬 <b>Cloud Pub/Sub</b><br/>Global Ingestion Bus"]
        Dataflow["⚙️ <b>Cloud Dataflow + DLP</b><br/>Deduplication, Normalization & PII Scrubbing"]
        BQ[("🗄️ <b>BigQuery Lakehouse & Feature Store</b><br/>Canonical Telemetry, OPM Baselines & Topology Graphs")]
        VectorDB[("🔍 <b>Vertex AI Vector Search</b><br/>SOP Runbook Embeddings Index")]
    end

    %% 3. AUTONOMOUS AGENT REASONING CORE
    subgraph GCP_AI["3. Autonomous Gemini SRE Agent Core (Vertex AI)"]
        direction TB
        AgentCore["🤖 <b>Autonomous Gemini SRE Agent</b><br/>4-Phase Reasoning: Triage ➔ RCA ➔ Diagnostics ➔ ITSM Dispatch"]
        
        subgraph ToolSuite["Supportive Agentic Tool Suite"]
            direction LR
            RAGTool["🔍 <b>RAG Search Tool</b>"]
            SandboxTool["⚡ <b>AST Sandbox Tool</b>"]
            GraphTool["🕸️ <b>Topology Graph Tool</b>"]
        end

        Guardrails["🛡️ <b>Model Armor & Context Caching</b><br/>Prompt injection defense & schema caching"]
    end

    %% 4. ACTION & ITSM
    subgraph Action["4. Enterprise Action Layer (ServiceNow)"]
        direction TB
        DispatchQueue["📬 <b>Cloud Tasks Dispatcher</b><br/>Rate-limited API shock absorber (Max 20 req/s)"]
        SNOW["🎫 <b>ServiceNow Incident Record</b><br/>Direct SRE Pod Routing, RCA & Attached Findings"]
        SRE["👨‍💻 <b>On-Call SRE Pods</b><br/>One-click runbook remediation & accelerated MTTR"]
        EvalStore[("📊 <b>BigQuery Evaluation Store</b><br/>Closed-loop SRE feedback & accuracy tracking")]
    end

    %% INTER-LAYER FLOWS
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
    SNOW -->|Page On-Call Engineer| SRE
    
    SNOW -.->|Incident Closure Feedback Webhook| EvalStore
    EvalStore -.->|Continuous Prompt & Embedding Tuning| AgentCore

    %% STYLING
    classDef s1 fill:#FFF3E0,stroke:#E65100,stroke-width:2px,color:#BF360C;
    classDef s2 fill:#FFEBEE,stroke:#C62828,stroke-width:2px,color:#B71C1C;
    classDef s3 fill:#E3F2FD,stroke:#1565C0,stroke-width:2px,color:#0D47A1;
    classDef s4 fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px,color:#1B5E20;
    classDef s5 fill:#ECEFF1,stroke:#37474F,stroke-width:2px,color:#263238;
    classDef gcpData fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px,color:#4A148C;
    classDef gcpAI fill:#EDE7F6,stroke:#303F9F,stroke-width:2px,color:#1A237E;
    classDef snow fill:#E0F2F1,stroke:#00796B,stroke-width:2px,color:#004D40;

    class Akamai s1;
    class Adobe s2;
    class Dyna s3;
    class GCPOps s4;
    class Splunk s5;
    class PubSub,Dataflow,BQ,VectorDB gcpData;
    class AgentCore,ToolSuite,Guardrails gcpAI;
    class DispatchQueue,SNOW,SRE,EvalStore snow;
```

---

## 🚀 Quick Navigation by Domain

| Role / Focus Area | Recommended Reading Path |
| :--- | :--- |
| **Enterprise AI Architects** | 1. [aiops_platform_overview.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/00_overview/aiops_platform_overview.md)<br/>2. [aiops_intelligence_layer.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/03_intelligence_and_reasoning/aiops_intelligence_layer.md)<br/>3. [data_processing_and_feature_store.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/02_storage_and_lakehouse/data_processing_and_feature_store.md)<br/>4. [servicenow_integration.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/04_itsm_and_remediation/servicenow_integration.md) |
| **Data & Pipeline Engineers** | 1. [ingestion_architecture.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/ingestion_architecture.md)<br/>2. [source_telemetry_matrix.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/source_telemetry_matrix.md)<br/>3. [data_contracts_and_schemas.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/data_contracts_and_schemas.md)<br/>4. [data_processing_and_feature_store.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/02_storage_and_lakehouse/data_processing_and_feature_store.md) |
| **SRE & Operations Engineers** | 1. [source_telemetry_matrix.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/source_telemetry_matrix.md)<br/>2. [servicenow_integration.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/04_itsm_and_remediation/servicenow_integration.md)<br/>3. [ingestion_best_practices.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/ingestion_best_practices.md) |
| **Security & Compliance** | 1. [data_contracts_and_schemas.md#dlp](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/data_contracts_and_schemas.md)<br/>2. [aiops_intelligence_layer.md#guardrails](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/03_intelligence_and_reasoning/aiops_intelligence_layer.md) |
