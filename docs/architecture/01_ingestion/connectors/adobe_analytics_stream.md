# Ingestion Connector: Adobe Analytics (Digital Experience & Business Telemetry)

## 1. Overview & Connector Role

**Adobe Analytics** captures customer journey events, checkout funnels, and real-time revenue KPIs across digital web portals and mobile applications.

The **Adobe Analytics Connector** bridges the gap between digital retail business health and IT operational monitoring. By ingesting streaming clickstream and business transaction events into GCP, the AIOps platform enables **Silent Outage Detection**—detecting when business revenue drops due to UI/client bugs even when backend infrastructure returns HTTP 200 OK.

```mermaid
flowchart TD
    subgraph Adobe_Digital["Adobe Experience Cloud"]
        direction TB
        AEP["⚡ <b>AEP Streaming Ingestion</b><br/>Live Cart & Order Events"]
        DataFeeds["📦 <b>Raw Clickstream Feeds</b><br/>Hourly Batch Feeds (GCS)"]
    end

    subgraph GCP_Ingestion["GCP Ingestion & Processing"]
        direction TB
        Proxy["🛡️ <b>AEP Gateway (Cloud Run)</b>"]
        PubSub["📬 <b>Cloud Pub/Sub</b><br/><code>telemetry.adobe.raw</code>"]
        Dataflow["⚙️ <b>Dataflow Pipeline</b>"]
        BQ[("🗄️ <b>BigQuery Lakehouse</b>")]
    end

    subgraph Intelligence["AIOps Business Intelligence"]
        direction TB
        BQML["📈 <b>BQML ARIMA_PLUS</b><br/>Continuous OPM Baseline"]
        Router["🧠 <b>Semantic Router</b>"]
        SNOW["🎫 <b>ServiceNow P1 Incident</b>"]
    end

    AEP -->|HTTP Streaming Push| Proxy
    DataFeeds -->|Direct Batch Upload| BQ
    Proxy --> PubSub
    PubSub --> Dataflow
    Dataflow --> BQ

    BQ -->|1-min OPM Aggregates| BQML
    BQML -->|Revenue Drop Anomaly| Router
    Router -->|P1 Business Outage Ticket| SNOW

    classDef a fill:#FFEBEE,stroke:#C62828,stroke-width:2px,color:#B71C1C;
    classDef g fill:#EDE7F6,stroke:#512DA8,stroke-width:2px,color:#311B92;
    classDef i fill:#E0F2F1,stroke:#00695C,stroke-width:2px,color:#004D40;

    class AEP,DataFeeds a;
    class Proxy,PubSub,Dataflow,BQ g;
    class BQML,Router,SNOW i;
```

---

## 2. Ingestion Mechanics

### 2.1 AEP Streaming Connector
* **Protocol**: Real-time HTTP POST event stream from Adobe Experience Platform (AEP) Edge Network to Cloud Run Gateway.
* **Latency**: 1 – 2 minutes from user browser interaction to BigQuery availability.
* **Event Filtering**: Only high-value conversion events (`purchase`, `scCheckout`, `scAdd`, `paymentError`) are streamed in real time.

### 2.2 Raw Clickstream Hourly Data Feeds
* **Delivery**: Compressed TSV/Parquet clickstream data feeds exported hourly from Adobe Data Warehouse to Google Cloud Storage (`gs://aiops-adobe-datafeeds/`).
* **Ingestion**: BigQuery Data Transfer Service (DTS) automatically loads feeds into partitioned analytical tables for historical baseline training.

---

## 3. Data Schema & Business KPI Mappings

| Adobe Raw Field | Canonical Field | Business KPI Description | ML Baseline Model |
| :--- | :--- | :--- | :--- |
| `hitid_high` + `hitid_low` | `event_id` | Unique clickstream hit identifier | Event deduplication |
| `date_time` | `timestamp` | UTC user interaction timestamp | Time-series forecasting |
| `events` (`event1`, `purchase`)| `metrics[opm]` | Orders Per Minute (OPM) count | BQML `ARIMA_PLUS` Anomaly Detection |
| `scAdd` / `scCheckout` | `business_context.funnel_stage` | Checkout Funnel progression | Funnel conversion drop rate |
| `revenue` | `business_context.estimated_revenue_loss_usd` | Transaction Gross Value ($) | Financial outage severity ranking |
| `user_agent` / `browser` | `business_context.user_cohort` | Client browser & OS version | Client-side release bug isolation |
| `payment_gateway_status` | `log_payload.error_code` | Third-party payment gateway error | Gateway vendor failover trigger |

---

## 4. Silent Outage Detection Architecture

1. **Continuous 1-Minute Aggregation**: Dataflow aggregates Orders Per Minute (OPM) and Cart Additions across 60-second tumbling windows.
2. **ARIMA_PLUS Evaluation**: BigQuery ML evaluates the real-time OPM against seasonal retail historical baselines (accounting for day-of-week, hour-of-day, and promotional events).
3. **Automated P1 Alerting**: If OPM drops below 3 standard deviations ($Z < -3.0$) for 3 consecutive minutes, a high-priority business incident is immediately pushed to ServiceNow with estimated dollar loss per minute.
