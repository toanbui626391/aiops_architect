# Interview Question Bank & Evaluation Frameworks

This directory contains interview guides, candidate evaluation scorecards, technical deep-dive analyses, and practical coding/architecture tests tailored for technical hiring in **AI Engineering, Data Engineering for AI, AIOps, and Autonomous Agentic Systems**.

---

## 1. Data Engineering for Production AI & Lakehouse Systems

1. [Data Engineer (AI & Production Big Data Systems) Interview Guide](file:///Users/toanbui/dev/aiops_architect/questions/data_engineer_ai_production_interview_guide.md)
   - **Primary 12-Question Evaluation Framework** for Senior, Staff, and Principal Data Engineers.
   - Covers 5 Core Domains: 500k EPS Streaming & Watermarks, Stateful Alert Windowing, High-Throughput Vector/Embedding Ingestion, HNSW vs IVF Indexing, Point-in-Time AS-OF Joins, Iceberg/BigQuery Partitioning & CDC, In-Flight Cloud DLP Scrubbing, Dead-Letter Queues & Schema Evolution, Petabyte Backfilling & Skew Salting, FinOps Slot Cost Governance, Airflow/Composer DAG Partial Failure Checkpointing, and Heterogeneous Feature Freshness SLAs.
   - Includes Context, Senior/Staff Green Flags, Junior Pitfalls (Red Flags), and Follow-Up Probes for every question.

2. [Data Engineer Follow-Up Probes: Deep-Dive Technical Analysis & Master Answers](file:///Users/toanbui/dev/aiops_architect/questions/data_engineer_follow_up_probes_deep_dive.md)
   - **Exhaustive Breakdown of all 12 Data Engineering Follow-Up Probes**.
   - Details interviewer psychology, trap detection, under-the-hood streaming/indexing mechanics, Mermaid architecture diagrams, mathematical formulations, and Staff-level master answers.

3. [Data Engineer Practical Tests: Live Coding, System Architecture & Take-Home Tests](file:///Users/toanbui/dev/aiops_architect/questions/data_engineer_ai_take_home_and_live_coding_tests.md)
   - **Live Coding Test (60 mins)**: Stateful streaming alert deduplicator & event-time sliding window signature aggregator with low watermark tracking and DLQ routing in Python.
   - **Live Architecture Whiteboard (60 mins)**: Multi-Cloud 500k EPS Streaming Lakehouse & Vector Ingestion Engine for Autonomous SRE Agents (with grading scorecard).
   - **Take-Home Case Study (2–4 hours)**: Production-ready semantic chunker & embedding synchronization pipeline with token-bucket rate-limiting, exponential backoff, DLQ, and embedding drift monitoring.

---

## 2. AI Engineering, Agentic Systems & AIOps

1. [AI Engineer (Production & Systems) Interview Guide](file:///Users/toanbui/dev/aiops_architect/questions/ai_engineer_production_interview_guide.md)
   - **Primary 11-Question Evaluation Framework** for Senior / Staff / Lead AI Engineers.
   - Covers agent architecture, loop mitigation, state durability, latency breakdown, indirect prompt injection defense, evaluation CI/CD, and OpenTelemetry observability.
   - Includes Context, Green Flags, Red Flags, and follow-up probes for every question.

2. [AI Engineer Follow-Up Probes: Deep-Dive Technical Analysis & Master Answers](file:///Users/toanbui/dev/aiops_architect/questions/follow_up_probes_deep_dive_analysis.md)
   - **Exhaustive Breakdown of all 11 AI Engineer Follow-Up Probes**.
   - Details interviewer psychology/traps, under-the-hood technical mechanics, ASCII architecture flowcharts, mathematical formulas, and Staff-level master answers for each probe.

3. [Agent State Recovery Architecture](file:///Users/toanbui/dev/aiops_architect/questions/agent_state_recovery_architecture.md)
   - Architectural blueprint for distributed state persistence, deterministic checkpointing, and idempotent replay in autonomous agent execution graphs.
