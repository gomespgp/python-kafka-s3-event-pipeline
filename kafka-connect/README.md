# 🔌 Kafka Connect & Connectors

This directory contains the declarative JSON configuration manifests and specifications for all **Kafka Connectors** deployed in the platform.

---

## 📖 Introduction to Kafka Connect

**Apache Kafka Connect** is a scalable, fault-tolerant runtime framework designed to stream data between Apache Kafka and external data systems (databases, object storage, search indexes, key-value stores) **without writing custom consumer/producer code**.

### Core Architecture Concepts:
- **Source Connectors:** Ingest data *from* external source systems (e.g. relational DB binlogs via Debezium) *into* Kafka topics.
- **Sink Connectors:** Stream data *from* Kafka topics *out to* destination storage sinks (e.g. MinIO S3 object storage, Elasticsearch, Snowflake).
- **Tasks:** Sub-workers created by the connector engine to parallelize data movement across topic partitions.
- **Sidecar Auto-Registration:** In this platform, connector JSON definitions in `connectors/` are automatically registered into the running `kafka-connect` service on container startup via `.docker/kafka-connect/init-connectors.sh`.

---

## 📋 Active Connector Registry

| Connector Name | Type | Class / Plugin | Target System | Topics Matched | Format |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **[`s3-sink-crm`](connectors/s3-sink-crm.json)** | Sink | `io.confluent.connect.s3.S3SinkConnector` | MinIO S3 (`kafka-s3-events-sink`) | `crm-.*` (Regex) | Newline JSON (NDJSON) |

---

## 🔍 Connector Details

### 1. `s3-sink-crm` (CRM Event S3 Sink)
Streams real-time CRM mutation events (`contacts`, `leads`, `deals`, `engagements`) from Kafka into partitioned files inside MinIO S3.

- **Configuration File:** [`connectors/s3-sink-crm.json`](connectors/s3-sink-crm.json)
- **Target Bucket:** `kafka-s3-events-sink`
- **Topic Selection:** Regex `crm-.*` (dynamically captures any topic prefixed with `crm-`)
- **Partitioner:** `TimeBasedPartitioner` generating Hive-style partitions:
  ```text
  s3://kafka-s3-events-sink/crm-<object_type>/year=YYYY/month=MM/day=DD/hour=HH/
  ```
- **Flush Triggers:**
  - `flush.size: 15` (flushes to S3 every 15 records per topic partition)
  - `rotate.interval.ms: 60000` (forces buffer flush every 60 seconds)
- **Storage Class:** `io.confluent.connect.s3.storage.S3Storage`
- **Output Format:** JSON (`io.confluent.connect.s3.format.json.JsonFormat`)

---

## 🛠️ How to Add a New Connector

To register a new connector (e.g. Debezium PostgreSQL Source Connector or another S3 Sink):

1. **Create a JSON manifest** in this `connectors/` directory (e.g. `connectors/postgres-cdc-source.json`):
   ```json
   {
     "name": "my-new-connector",
     "config": {
       "connector.class": "io.confluent.connect.s3.S3SinkConnector",
       "tasks.max": "1",
       ...
     }
   }
   ```
2. **Restart the stack or run `init-connectors.sh`:**
   ```bash
   make restart service=kafka-connect-init
   ```
3. **Or manually register/update via the REST API:**
   ```bash
   curl -X PUT "http://localhost:8083/connectors/my-new-connector/config" \
     -H "Content-Type: application/json" \
     -d @connectors/my-new-connector.json
   ```

---

## 📡 Useful Kafka Connect REST API Commands

| Action | HTTP Request |
| :--- | :--- |
| **List all connectors** | `GET http://localhost:8083/connectors` |
| **Check connector status** | `GET http://localhost:8083/connectors/s3-sink-crm/status` |
| **Get connector config** | `GET http://localhost:8083/connectors/s3-sink-crm/config` |
| **Pause connector** | `PUT http://localhost:8083/connectors/s3-sink-crm/pause` |
| **Resume connector** | `PUT http://localhost:8083/connectors/s3-sink-crm/resume` |
| **Restart failed tasks** | `POST http://localhost:8083/connectors/s3-sink-crm/tasks/0/restart` |
| **Delete connector** | `DELETE http://localhost:8083/connectors/s3-sink-crm` |

---

## 📚 Related Documentation

- **[Kafka Broker & Connect Deep Dive](../docs/architecture/kafka-broker-connect.md)** — Architectural explanation of KRaft, buffer triggers, and workers.
- **[Configuration Reference](../docs/reference/configuration.md)** — Full parameter-by-parameter breakdown of `s3-sink-crm.json`.
