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
    │   ├── data_contracts_and_schemas.md              # 📜 Canonical Event Schemas (CEF/JSON/Parquet) & Cloud DLP
    │   └── connectors/
    │       ├── akamai_datastream.md                   # 🌐 Akamai DataStream 2 (Edge/Security) Ingestion
    │       ├── dynatrace_ingestion.md                 # ⚡ Dynatrace (APM/PurePath/Smartscape) Ingestion
    │       ├── gcp_ops_ingestion.md                   # ☁️ GCP Operations Suite (GKE/Infrastructure) Ingestion
    │       ├── splunk_hec_ingestion.md                # 📜 Splunk HEC (Enterprise Logs & SIEM) Ingestion
    │       └── adobe_analytics_stream.md              # 🛍️ Adobe Analytics (Clickstream & Business KPIs) Ingestion
    ├── 02_storage_and_lakehouse/
    │   ├── lakehouse_architecture.md                  # 🗄️ BigQuery Lakehouse, Parquet on GCS, Partitioning & Retention
    │   └── data_processing_and_feature_store.md       # ⚡ Stateful Alert Windowing, Feature Store, Graph ETL & RAG Vectorization
    ├── 03_intelligence_and_reasoning/
    │   └── aiops_intelligence_layer.md                # 🧠 Semantic Router, Gemini SRE Agent & BQML Anomaly Detection
    └── 04_itsm_and_remediation/
        └── servicenow_integration.md                  # 🎫 ServiceNow ITSM Integration, CMDB Sync & Automated SOPs
```

---

## 🗺️ High-Level Platform Map

```mermaid
flowchart TD
    %% 1. SRE OBSERVABILITY ECOSYSTEM
    subgraph Sources["1. SRE Observability Sources"]
        direction LR
        Akamai["🌐 <b>Akamai</b><br/>Edge & Security"]
        Adobe["🛍️ <b>Adobe Analytics</b><br/>Business Funnel"]
        Dyna["⚡ <b>Dynatrace</b><br/>APM & Trace Graph"]
        GCPOps["☁️ <b>GCP Ops Suite</b><br/>Cloud & GKE Infra"]
        Splunk["📜 <b>Splunk</b><br/>Logs & Forensics"]
    end

    %% 2. INGESTION & DATA LAKEHOUSE (GCP)
    subgraph GCP_Data["2. GCP Ingestion & Unified Lakehouse"]
        direction TB
        PubSub["📬 <b>Cloud Pub/Sub</b><br/>Global Ingestion Bus"]
        Dataflow["⚙️ <b>Cloud Dataflow + DLP</b><br/>Deduplication, Normalization & PII Scrubbing"]
        BQ[("🗄️ <b>BigQuery Lakehouse</b><br/>Canonical Telemetry & Baseline Tables")]
        VectorDB[("🔍 <b>Vertex AI Vector Search</b><br/>SOP Runbook Embeddings Index")]
    end

    %% 3. AIOps REASONING & INTELLIGENCE
    subgraph GCP_AI["3. AIOps Intelligence Layer (Vertex AI)"]
        direction TB
        BQML["📈 <b>Business Anomaly Engine</b><br/>BQML ARIMA_PLUS Models"]
        SemanticRouter["🧠 <b>Semantic Router</b><br/>Alert Correlation & De-noising"]
        ContextAgent["🤖 <b>Context-Aware SRE Agent</b><br/>Gemini Multi-Source RCA"]
        SOPEngine["⚡ <b>Automated SOP Engine</b><br/>Diagnostic Execution"]
    end

    %% 4. ACTION & ITSM
    subgraph Action["4. Enterprise Action Layer"]
        direction TB
        SNOW["🎫 <b>ServiceNow ITSM</b><br/>Smart Incident Routing & CMDB Sync"]
        SRE["👨‍💻 <b>On-Call SRE Pods</b><br/>Accelerated MTTR & Remediation"]
    end

    %% INTER-LAYER FLOWS
    Akamai -->|DataStream 2 Push| PubSub
    Adobe -->|AEP Stream Ingestion| PubSub
    Dyna -->|Problem Webhooks & OTel| PubSub
    GCPOps -->|Log Sinks & GMP Scraper| PubSub
    Splunk -->|HEC Event Forwarding| PubSub

    PubSub --> Dataflow
    Dataflow -->|Stream Clean Telemetry| BQ
    Dataflow -->|Alert Stream| SemanticRouter

    BQ -->|Continuous Baseline Evaluation| BQML
    BQML -->|Business Anomaly Alert| SemanticRouter
    VectorDB <-->|Retrieve SOP Runbook| ContextAgent
    SemanticRouter -->|Correlated Alert Context| ContextAgent
    ContextAgent --> SOPEngine

    SemanticRouter -->|Direct SRE Pod Routing| SNOW
    ContextAgent -->|Enriched RCA & Forensic Context| SNOW
    SOPEngine <-->|Execute Diagnostics & Attach Output| SNOW
    SNOW -->|Page On-Call Engineer| SRE

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
    class BQML,SemanticRouter,ContextAgent,SOPEngine gcpAI;
    class SNOW,SRE snow;
```

---

## 🚀 Quick Navigation by Domain

| Role / Focus Area | Recommended Reading Path |
| :--- | :--- |
| **Enterprise AI Architects** | 1. [aiops_platform_overview.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/00_overview/aiops_platform_overview.md)<br/>2. [aiops_intelligence_layer.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/03_intelligence_and_reasoning/aiops_intelligence_layer.md)<br/>3. [data_processing_and_feature_store.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/02_storage_and_lakehouse/data_processing_and_feature_store.md)<br/>4. [lakehouse_architecture.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/02_storage_and_lakehouse/lakehouse_architecture.md) |
| **Data & Pipeline Engineers** | 1. [ingestion_architecture.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/ingestion_architecture.md)<br/>2. [data_contracts_and_schemas.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/data_contracts_and_schemas.md)<br/>3. [data_processing_and_feature_store.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/02_storage_and_lakehouse/data_processing_and_feature_store.md)<br/>4. [lakehouse_architecture.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/02_storage_and_lakehouse/lakehouse_architecture.md) |
| **SRE & Tool Administrators** | 1. [Connectors Directory](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/connectors/)<br/>2. [servicenow_integration.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/04_itsm_and_remediation/servicenow_integration.md)<br/>3. [ingestion_best_practices.md](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/ingestion_best_practices.md) |
| **Security & Compliance** | 1. [data_contracts_and_schemas.md#dlp](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/data_contracts_and_schemas.md)<br/>2. [ingestion_architecture.md#security](file:///c:/Users/ToanBX/dev/personal/aiops_architect/docs/architecture/01_ingestion/ingestion_architecture.md) |
