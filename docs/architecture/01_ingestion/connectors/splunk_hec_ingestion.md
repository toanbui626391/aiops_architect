# Ingestion Connector: Splunk (Enterprise Logs & SIEM)

## 1. Overview & Connector Role

**Splunk** serves as the central enterprise logging and Security Information and Event Management (SIEM) platform, aggregating high-volume unstructured logs across legacy on-premise data centers, physical Point-of-Sale (POS) warehouse systems, middleware brokers (Kafka/IBM MQ), and enterprise SAP backends.

The **Splunk Connector** operates via two complementary patterns:
1. **Push Forwarding (Splunk HEC)**: Real-time forwarding of high-priority error logs, security notables, and POS anomalies to **Cloud Pub/Sub**.
2. **On-Demand Forensic Query Proxy**: Context-aware REST API queries executed autonomously by the Gemini SRE Agent during an active incident to extract $\pm 10$ minutes of deep server logs.

```mermaid
flowchart TD
    subgraph Splunk_Fleet["Splunk Enterprise Fleet"]
        direction TB
        Splunk_Indexers["📚 <b>Splunk Indexers</b><br/>Enterprise, POS & SIEM Logs"]
        Splunk_SearchHead["🔍 <b>Search Head / REST API</b><br/>Targeted SPL Query Execution"]
    end

    subgraph GCP_Ingress["GCP Ingestion & Proxy Layer"]
        direction TB
        HEC_Proxy["🛡️ <b>HEC Proxy (Cloud Run)</b><br/>Token Auth & Rate Limiting"]
        ForensicProxy["🤖 <b>Forensic Query Proxy (Cloud Run)</b><br/>Targeted SPL Generator"]
        PubSub["📬 <b>Cloud Pub/Sub</b><br/><code>telemetry.splunk.raw</code>"]
    end

    subgraph GCP_Core["GCP Intelligence Core"]
        direction TB
        Dataflow["⚙️ <b>Dataflow Pipeline</b>"]
        ContextAgent["🤖 <b>Context-Aware SRE Agent</b>"]
        SNOW["🎫 <b>ServiceNow Ticket</b>"]
    end

    Splunk_Indexers -->|HTTPS Push (HEC Forwarder)| HEC_Proxy
    HEC_Proxy --> PubSub
    PubSub --> Dataflow

    ContextAgent -->|1. Trigger SPL Query (time ±10m)| ForensicProxy
    ForensicProxy <-->|2. Execute SPL via REST API| Splunk_SearchHead
    ForensicProxy -->|3. Attach Summarized Forensics| SNOW

    classDef s fill:#ECEFF1,stroke:#37474F,stroke-width:2px,color:#263238;
    classDef g fill:#EDE7F6,stroke:#512DA8,stroke-width:2px,color:#311B92;
    classDef a fill:#E0F2F1,stroke:#00695C,stroke-width:2px,color:#004D40;

    class Splunk_Indexers,Splunk_SearchHead s;
    class HEC_Proxy,ForensicProxy,PubSub,Dataflow,ContextAgent g;
    class SNOW a;
```

---

## 2. Ingestion Mechanics

### 2.1 Splunk HTTP Event Collector (HEC) Forwarder
* **Push Mechanism**: Splunk Heavy Forwarders forward alert events and filtered error logs using standard HEC JSON formatting.
* **Authentication**: Authorization header with pre-shared HEC token validated against **GCP Secret Manager**.
* **Filtering at Splunk Source**: Heavy forwarders drop non-critical debug/info logs at the enterprise boundary to reduce cross-cloud egress costs.

### 2.2 SRE Forensic Query Proxy
When a P1/P2 incident is triggered in ServiceNow:
1. The **Context-Aware SRE Agent** dynamically constructs a targeted Splunk Processing Language (SPL) query:
   ```spl
   index=retail_pos host="store-842-pos-*" earliest=-10m latest=+10m "ERROR" OR "FATAL"
   | stats count by exception_type, error_code
   ```
2. The proxy queries Splunk's REST API (`https://splunk-sh.retail.internal:8089/services/search/jobs`), summarizes the top 10 error signatures using Vertex AI, and posts the summary directly to the ServiceNow ticket.

---

## 3. Data Schema & Field Mappings

| Raw Splunk CIM Field | Canonical Field | Description | AI/ML Use Case |
| :--- | :--- | :--- | :--- |
| `_cd` / `event_id` | `event_id` | Splunk event identifier | Traceability & deduplication |
| `_time` | `timestamp` | UTC event timestamp | Chronological correlation |
| `source` / `index` | `entity.service_name` | Originating log source (e.g., `pos_orders`) | System domain categorization |
| `host` | `entity.host` | Physical server or store terminal ID | Warehouse/store isolation |
| `_raw` | `log_payload.message` | Raw unstructured log string (DLP sanitized)| Vertex AI semantic embedding & clustering |
| `error_code` | `log_payload.error_code` | Application specific error code | Automated runbook matching |
| `store_id` | `business_context.user_cohort` | Physical store location number | Store-specific hardware failure detection |

---

## 4. Cost Optimization & Egress Management

* **Smart Forwarding**: Only logs with `severity >= ERROR` or matching active security threat signatures are pushed to GCP Pub/Sub in real time.
* **Targeted Pulls**: Deep historical logs remain indexed in Splunk on-premise storage and are only queried on demand during live incident triage, reducing unnecessary data transfer costs.
