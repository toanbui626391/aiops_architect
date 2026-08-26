# Ingestion Connector: Akamai DataStream 2 (Edge & Security)

## 1. Overview & Connector Role

**Akamai** serves as the digital perimeter for the global retail enterprise, handling user requests across 4,000+ edge locations worldwide. 

The **Akamai DataStream 2 Connector** captures real-time HTTP edge performance, TLS negotiation metrics, Web Application Firewall (WAF) triggers, and Bot Management scores, streaming them directly into the GCP AIOps Core with sub-10 second latency.

```mermaid
flowchart LR
    AkamaiEdge["🌐 <b>Akamai Edge CDN</b><br/>Global Edge Nodes"] -->|HTTPS Push (DataStream 2)| CloudRunProxy["🛡️ <b>Cloud Run Proxy</b><br/>HMAC & Token Verification"]
    CloudRunProxy -->|Stream JSON Batches| PubSub["📬 <b>Cloud Pub/Sub</b><br/><code>telemetry.akamai.raw</code>"]
    PubSub --> Dataflow["⚙️ <b>Dataflow Pipeline</b><br/>Canonical Normalization & DLP"]

    classDef a fill:#FFF3E0,stroke:#E65100,stroke-width:2px,color:#BF360C;
    classDef g fill:#EDE7F6,stroke:#512DA8,stroke-width:2px,color:#311B92;

    class AkamaiEdge a;
    class CloudRunProxy,PubSub,Dataflow g;
```

---

## 2. Ingestion Architecture & Data Flow

### 2.1 Push Mechanism (Akamai DataStream 2)
* **Delivery Method**: HTTPS Push directly to a dedicated Cloud Run endpoint (`https://ingest-akamai.aiops.retail.internal/v1/stream`).
* **Format**: Gzip-compressed, chunked JSON payload streams.
* **Batching Window**: 5 seconds or 5 MB payload buffer.

### 2.2 Ingress Gateway (Cloud Run)
The Cloud Run proxy acts as the secure perimeter for Akamai pushes:
1. **Authentication**: Validates the Akamai custom authorization token and HMAC-SHA256 signature in the `X-Akamai-Signature` header against secrets stored in **GCP Secret Manager**.
2. **Buffering**: Publishes batches asynchronously into `telemetry.akamai.raw`.
3. **Acknowledgment**: Responds with `HTTP 200 OK` within 200ms to avoid Akamai delivery retries.

---

## 3. Data Schema & Extracted Fields

| Raw Akamai Field | Canonical Field | Description | AI/ML Use Case |
| :--- | :--- | :--- | :--- |
| `reqId` | `event_id` | Unique request tracking ID | End-to-end request tracing |
| `start` | `timestamp` | Epoch millisecond timestamp | Time-series alignment |
| `cpCode` | `entity.service_name` | Content Provider Code (e.g., `CP_CHECKOUT_PROD`) | Service domain routing |
| `statusCode` | `log_payload.error_code` | HTTP Response code (200, 502, 504) | Origin saturation detection |
| `turnAroundTimeMSec` | `metrics[ttfb]` | Time to First Byte (TTFB) in ms | Edge latency anomaly modeling |
| `wafRuleId` | `raw_attributes.waf_rule` | Triggered WAF rule identifier | DDoS & bot attack correlation |
| `botScore` | `metrics[bot_score]` | Akamai Bot Manager score (0–100) | Bot surge vs. legitimate user spike filtering |
| `country` | `entity.region` | Client 2-letter ISO country code | Regional outage detection |
| `originResponseTimeMSec`| `metrics[origin_latency]` | Origin backend processing duration in ms | Cloud origin backpressure forecasting |

---

## 4. Resilience & Error Handling

* **Upstream Retries**: If Cloud Run returns HTTP 5xx or times out (> 10s), Akamai buffers logs at the edge for up to 24 hours and retries with exponential backoff.
* **Pub/Sub Dead-Letter Queue**: If Dataflow encounters corrupted JSON or schema violations, records route to `telemetry.akamai.dlq`.
* **GCS Cold Storage Tiering**: Aggregated hourly logs are automatically compressed and saved to `gs://aiops-telemetry-archive/akamai/YYYY/MM/DD/` in Parquet format.
