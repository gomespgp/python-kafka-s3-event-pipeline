# 🚀 Project Roadmap & Enhancement Backlog

This document tracks planned architecture enhancements, technical debt, and roadmap features for the **Python Kafka S3 Event Pipeline**.

---

## 📌 Phase 0: FastAPI Producer Refactoring & Best Practices (✅ Completed)

- [x] **Modular FastAPI Application Architecture**
  - [x] **Decouple App Routes into API Routers:** Split monolithic `main.py` into dedicated APIRouters (`api/v1/endpoints/webhooks.py` and `api/v1/endpoints/simulation.py`).
  - [x] **Environment Configuration Management:** Replace raw `os.getenv` calls with `pydantic-settings` (`BaseSettings`) in `core/config.py`.
  - [x] **Layered Architecture Separation:**
    - `core/`: Kafka Producer client singleton, settings, and lifecycle managers.
    - `schemas/`: Pydantic models for webhook request payloads, JSON envelopes, and API responses.
    - `services/`: Business logic for mock event generation and background async workers.
  - [x] **Target Directory Layout:**
    ```text
    producer/app/
    ├── main.py                   # FastAPI initialization & lifespan context
    ├── core/
    │   ├── config.py             # Pydantic BaseSettings (.env parsing)
    │   └── kafka.py              # Kafka Producer client & delivery callbacks
    ├── api/
    │   └── v1/
    │       ├── router.py         # Main API router inclusion
    │       └── endpoints/
    │           ├── webhooks.py   # Webhook ingestion routes (/webhooks/crm/{type})
    │           └── simulation.py # Simulator routes (/simulation/*)
    ├── schemas/
    │   ├── events.py             # WebhookPayload, EventEnvelope, Entity schemas
    │   └── responses.py          # Standardized API response models
    └── services/
        ├── generator.py          # Background asyncio simulation loop
        └── mock_factory.py       # Faker data generators for CRM entities
    ```

---

## 📌 Phase 1: Ingestion & Change Data Capture (CDC)

- [ ] **PostgreSQL CDC Strategy with Debezium**
  - [ ] Add a PostgreSQL operational database container (`postgres_source`).
  - [ ] Add an initialization service with DDL schemas and seed transactional data.
  - [ ] Create a Python script or API endpoint to randomly generate `INSERT`, `UPDATE`, and `DELETE` database mutations.
  - [ ] Configure a Debezium PostgreSQL Source Connector in Kafka Connect to stream binlog WAL mutations into Kafka topics and land them in S3 under table-specific topics.

---

## 📌 Phase 2: Data Lakehouse & Transformation (Medallion Architecture)

- [ ] **Medallion Storage Layout (Bronze → Silver → Gold)**
  - [ ] **Silver Layer (Cleaned & Typed):** Build a processing job (using PySpark or DuckDB) to read raw JSON from MinIO Bronze (`kafka-s3-events-sink`), deduplicate records by `event_id`, enforce strict data types, and output snappy-compressed **Parquet** files partitioned by date and entity.
  - [ ] **Gold Layer (Aggregated Business Models):** Implement dimensional models and aggregated analytical views (e.g., *Daily Closed Sales Volume*, *Lead Conversion Rates*, *Sales Rep Performance*) using dbt-duckdb or DuckDB.

---

## 📌 Phase 3: Real-Time Stream Processing & Schema Enforcement

- [ ] **Stream Processing Engine (Flink / PySpark / Bytewax)**
  - [ ] Introduce a stream processing engine to compute real-time sliding window metrics (e.g., alert if `deal.closed_lost` events spike within a 5-minute window).
  - [ ] Enrich incoming CRM events in motion (e.g., join `contacts` events with `leads` events before landing into S3).
- [ ] **Schema Registry & Contract Enforcement (Avro / Protobuf)**
  - [ ] Integrate Confluent Schema Registry into Docker Compose.
  - [ ] Update Python producer and Kafka Connect to use Avro/Protobuf serializers with strict backward compatibility checks to prevent schema drift.

---

## 📌 Phase 4: Observability, Quality & Management UIs

- [ ] **Connector Management UI**
  - [ ] Evaluate and integrate a built-in community UI tool for inspecting, pausing, and restarting Kafka Connectors interactively without manual REST calls.
- [ ] **Data Quality Guardrails & Dead Letter Queue (DLQ)**
  - [ ] Implement automated data quality validation tests (using Soda Core or Great Expectations) over S3 Parquet partitions.
  - [ ] Configure a Dead Letter Queue (`crm-dlq`) topic and landing bucket (`s3://kafka-s3-events-sink/dlq/`) for malformed or unparseable payload envelopes.
- [ ] **Analytics & BI Dashboards (Apache Superset)**
  - [ ] Connect Apache Superset with DuckDB (`httpfs`) to query MinIO S3 Parquet files directly.
  - [ ] Build interactive executive dashboards for live CRM KPIs and pipeline metrics.

---

## 📌 Phase 5: Security & Infrastructure

- [ ] **Secrets Management Integration**
  - [ ] Add a HashiCorp Vault container service (or Doppler) for dynamic container credential resolution and secret rotation.
- [ ] **Cloud Provisioning & IaC (Terraform)**
  - [ ] Create Terraform manifests to provision AWS MSK (Managed Kafka), AWS S3, and ECS/EKS clusters.