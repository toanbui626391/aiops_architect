# Ingestion Connector: GCP Operations Suite (Infrastructure & Platform)

## 1. Overview & Connector Role

The **Google Cloud Operations Suite Connector** provides real-time, native ingestion of telemetry generated across the enterprise's Google Cloud infrastructure, including **Google Kubernetes Engine (GKE)** clusters, serverless workloads (**Cloud Run**), data processing pipelines (**Cloud Dataflow**, **Dataproc**), and messaging backbones (**Cloud Pub/Sub**).

Because telemetry resides natively within Google Cloud, ingestion achieves **sub-second latency with zero egress cost**, providing foundational health metrics and audit logs for the entire AIOps platform.



---

## 2. Ingestion Mechanics & Configurations

### 2.1 Log Router Sinks (Cloud Logging)
* **Log Sink Name**: `sink-aiops-canonical-telemetry`
* **Destination**: `pubsub.googleapis.com/projects/aiops-prod/topics/telemetry.gcp.raw`
* **Inclusion Filter**:
  ```sql
  resource.type=("k8s_container" OR "k8s_pod" OR "k8s_cluster" OR "cloud_run_revision" OR "dataflow_step" OR "pubsub_subscription")
  AND (severity >= "WARNING" OR jsonPayload.error != "" OR protoPayload.methodName != "")
  ```

### 2.2 Managed Service for Prometheus (GMP) Metrics Scraper
* **Scrape Frequency**: 15 seconds.
* **Target Metrics**:
  * `container_cpu_usage_seconds_total`
  * `container_memory_working_set_bytes`
  * `kube_pod_container_status_waiting_reason{reason="CrashLoopBackOff"}`
  * `pubsub.googleapis.com/subscription/oldest_unacked_message_age`
  * `dataflow.googleapis.com/job/system_lag`

---

## 3. Data Schema & Canonical Mappings

| GCP Ops Native Field | Canonical Field | Description | AI/ML Use Case |
| :--- | :--- | :--- | :--- |
| `insertId` | `event_id` | Globally unique log record ID | Deduplication in Dataflow |
| `timestamp` | `timestamp` | UTC event timestamp | Chronological cross-tool alignment |
| `resource.labels.pod_name`| `entity.container_name` | GKE Pod identifier | Microservice instance isolation |
| `resource.labels.namespace_name` | `entity.service_name` | Kubernetes namespace/service | SRE pod routing |
| `severity` | `severity` | `DEFAULT`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` | Alert urgency scoring |
| `protoPayload.methodName`| `raw_attributes.audit_action`| Cloud IAM / Admin Activity method | CI/CD deployment & rogue config correlation |
| `jsonPayload.message` | `log_payload.message` | Sanitized application stdout/stderr | NLP log clustering in Vertex AI |

---

## 4. Operational Value & Failure Prevention

1. **CrashLoopBackOff Immediate Triage**: Detects misconfigured Kubernetes deployments or OOM kills within 3 seconds, triggering immediate rollback runbooks.
2. **Pipeline Saturation Forecasting**: BQML time-series forecasting monitors Pub/Sub message backlog age and proactively scales Dataflow workers before ingestion bottlenecks occur.
