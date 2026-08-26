# Ingestion Connector: Dynatrace (APM, Tracing & Topology)

## 1. Overview & Connector Role

**Dynatrace** provides full-stack Application Performance Monitoring (APM), distributed code-level tracing (PurePath), runtime dependency mapping (Smartscape), and AI-driven root cause detection (Davis AI Engine).

The **Dynatrace Connector** provides a dual-channel ingestion pipeline into GCP:
1. **Real-Time Problem & RCA Stream**: Real-time event webhooks triggered by Davis AI when anomalies or SLO breaches occur.
2. **Periodic Topology Graph Synchronization**: Scheduled REST API synchronization that updates the runtime dependency topology in BigQuery.
3. **OpenTelemetry Trace Export**: High-volume span and trace forwarding for error and p99 latency calls.



---

## 2. Ingestion Mechanics & Configurations

### 2.1 Davis AI Problem Webhook Integration
* **Trigger**: Automatic problem creation, status updates, and resolution notifications from Dynatrace Davis AI.
* **Payload Structure**: Custom JSON webhook payload containing `ProblemID`, `ProblemTitle`, `ImpactedEntities`, `RootCauseEntity`, `SeverityLevel`, and deep-link URLs.
* **Ingress**: Sent via HTTPS POST to Cloud Run with Bearer Token validation against **GCP Secret Manager**.

### 2.2 Smartscape Topological Entity Sync
* **Execution**: Scheduled Cloud Run Job running every 15 minutes.
* **API Endpoints**:
  * `GET /api/v2/entities?entitySelector=type(SERVICE)`
  * `GET /api/v2/entities?entitySelector=type(KUBERNETES_SERVICE)`
  * `GET /api/v2/entityTrees`
* **Storage**: Updates the `dynatrace_service_topology` table in BigQuery, enabling Graph Neural Network (GNN) dependency models and LLM contextual reasoning.

---

## 3. Data Schema & Field Mappings

| Dynatrace Payload Field | Canonical Field | Description | AI Operations Usage |
| :--- | :--- | :--- | :--- |
| `ProblemID` | `event_id` | Unique Dynatrace problem identifier | Ticket de-duplication in ServiceNow |
| `StartTime` | `timestamp` | Problem start epoch ms | Incident chronological ordering |
| `ImpactedEntity` | `entity.service_name` | Affected microservice or host | SRE pod ownership lookup |
| `RootCauseEntity` | `raw_attributes.root_cause_node` | Specific node/DB determined by Davis AI | Instant RCA verification |
| `ProblemSeverity` | `severity` | `AVAILABILITY`, `ERROR`, `PERFORMANCE` | P1/P2/P3 severity mapping |
| `PurePathTraceId` | `log_payload.trace_id` | OpenTelemetry distributed trace ID | Correlating logs across Splunk/GCP |
| `HeapUsagePercent` | `metrics[heap_usage]` | JVM/Node.js memory saturation | Memory leak & OOM kill prediction |

---

## 4. Operational Value in AIOps

1. **Deterministic Root Cause Confirmation**: Davis AI identifies code-level and database query bottlenecks, eliminating trial-and-error investigations for SREs.
2. **Context Enrichment in ServiceNow**: The AI Agent pulls PurePath stack traces and attaches them as formatted markdown directly to the ServiceNow incident.
