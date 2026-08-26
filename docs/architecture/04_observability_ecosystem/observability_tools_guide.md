# SRE Observability Tools Reference Guide

## 1. Executive Overview

This reference guide details the role, emitted telemetry, ingestion channels, and AI/ML operational capabilities of the SRE team's 5 core observability tools: **Akamai**, **Dynatrace**, **Google Cloud Operations Suite**, **Splunk**, and **Adobe Analytics**.

---

## 2. In-Depth Tool Specifications

### 2.1 Akamai (Edge & Perimeter Observability)
* **Domain**: Edge CDN, Network Perimeter, WAF, Bot Security.
* **Emitted Telemetry**: HTTP edge logs, TTFB (Time to First Byte), TLS handshake times, WAF triggers, DDoS mitigation vectors, Bot Manager classification scores.
* **Ingestion Channel**: Akamai **DataStream 2** HTTPS Push to Cloud Run Gateway ➔ `telemetry.akamai.raw`.
* **AI/ML Use Cases**:
  1. Edge latency anomaly clustering (regional ISP/routing degradation).
  2. Bot surge filtering to prevent false-positive autoscaling alerts.
  3. Time-series forecasting of origin saturation.

### 2.2 Dynatrace (APM & Distributed Tracing)
* **Domain**: Code-level profiling, distributed tracing (PurePath), service dependency graphs (Smartscape), Davis AI Engine.
* **Emitted Telemetry**: OpenTelemetry spans, GC pause times, database query durations, unhandled exceptions, Davis AI root cause problem webhooks.
* **Ingestion Channel**: Webhooks to Cloud Run Gateway ➔ `telemetry.dynatrace.raw`; scheduled REST API pull (`/api/v2/entities`) for Smartscape graph updates in BigQuery.
* **AI/ML Use Cases**:
  1. Graph Neural Networks (GNN) on Smartscape topology for cascading failure prediction.
  2. LLM semantic extraction from stack traces for automated ticket root cause summaries.
  3. Automated SOP diagnostic parameter population from SQL trace spans.

### 2.3 Google Cloud Operations Suite (Cloud & Platform Infrastructure)
* **Domain**: Native GCP infrastructure, GKE clusters, Dataflow pipelines, Pub/Sub message queues, BigQuery jobs.
* **Emitted Telemetry**: Container CPU/Memory utilization, `CrashLoopBackOff` restart counts, Pub/Sub backlog message age, Dataflow system lag, Cloud Audit Logs.
* **Ingestion Channel**: Native **Cloud Logging Log Router Sinks** ➔ `telemetry.gcp.raw`; Managed Service for Prometheus (GMP) metrics scraper.
* **AI/ML Use Cases**:
  1. Real-time GKE CrashLoopBackOff triage and automated rollback runbook triggering.
  2. BQML time-series forecasting on Pub/Sub backlog age to dynamically autoscale Dataflow workers.
  3. Cloud Audit Log anomaly detection to identify rogue CI/CD deployments.

### 2.4 Splunk (Enterprise Logging & Security Intelligence)
* **Domain**: Enterprise log hub, physical Point-of-Sale (POS) warehouse logs, middleware brokers (Kafka/IBM MQ), SIEM events.
* **Emitted Telemetry**: Linux/Windows OS event logs, active directory authentication, POS terminal logs, Splunk Enterprise Security (ES) notable events.
* **Ingestion Channel**: Splunk **HTTP Event Collector (HEC)** ➔ `telemetry.splunk.raw`; on-demand REST API for targeted SRE forensic queries ($\pm 10$ minutes).
* **AI/ML Use Cases**:
  1. Cross-domain log semantic embedding (clustering unstructured error messages across 50+ services).
  2. Contextual forensic log enrichment for active ServiceNow incident tickets.
  3. Correlating IT infrastructure failures with concurrent security alerts.

### 2.5 Adobe Analytics (Digital Experience & Business Telemetry)
* **Domain**: Customer journeys, clickstream interactions, checkout funnels, digital revenue metrics.
* **Emitted Telemetry**: Real-time clickstream events, Orders Per Minute (OPM), Cart Additions (`scAdd`), Checkout Drops (`scCheckout`), payment gateway failure rates.
* **Ingestion Channel**: Adobe Experience Platform (AEP) Streaming Ingestion ➔ `telemetry.adobe.raw`; hourly raw data feeds to GCS.
* **AI/ML Use Cases**:
  1. **Silent Outage Detection**: BigQuery ML `ARIMA_PLUS` models monitoring Orders Per Minute (OPM) to detect UI/client checkout bugs that infrastructure monitoring misses.
  2. **Financial Severity Ranking**: Calculating live dollar revenue loss per minute to prioritize P1 incident triage in ServiceNow.
  3. Customer cohort isolation (e.g., outages affecting only iOS v18.2 users).

---

## 3. Tool Comparison & Operational Matrix

| Dimension | Akamai | Dynatrace | GCP Ops Suite | Splunk | Adobe Analytics |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Telemetry Layer** | Edge / CDN / Perimeter | Application / Code / Traces | Cloud Infra & Platform | Enterprise Logs & SIEM | Business / User Funnel |
| **Ingestion SLA** | Sub-10s | Sub-5s | Sub-second – 1m | 1 – 3 min | 1 – 2 min |
| **Peak Throughput**| 150,000 EPS | 80,000 EPS | 100,000 EPS | 90,000 EPS | 40,000 EPS |
| **Primary Format** | JSON DataStream | PurePath JSON / Webhook | Cloud Logging JSON | CIM JSON / HEC | Clickstream HIT records |
| **Primary AI Input**| Edge anomaly vectors | Topology graph, traces | Utilization time-series | Unstructured log text | Business KPI time-series |
| **ServiceNow Role**| Edge latency alert | Code defect & RCA summary| Node capacity / crash alert| Deep forensic log snippet | P1 Business Outage ticket |
