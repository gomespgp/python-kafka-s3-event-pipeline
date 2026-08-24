# ⚡ Python Kafka S3 Event Pipeline

An end-to-end real-time event streaming and ingestion platform leveraging Apache Kafka (KRaft mode), Python (FastAPI + `confluent-kafka`), Kafka Connect S3 Sink, and MinIO object storage to stream CRM webhooks and events into a partitioned Data Lake S3 landing zone.

---

## 📌 Features

- **Real-Time Streaming:** High-throughput event ingestion using Confluent Apache Kafka in ZooKeeper-less KRaft mode.
- **FastAPI Producer & Simulator:** Webhook ingestion API and asynchronous background event simulator generating realistic CRM events (`contacts`, `leads`, `deals`, `engagements`).
- **Automated S3 Sink Connector:** Confluent Kafka Connect S3 Sink automatically registered on startup via a lightweight container sidecar.
- **Time-Based Partitioning:** Dynamic object storage directory layout in MinIO (`kafka-s3-events-sink/crm-<type>/year=YYYY/month=MM/day=DD/hour=HH/`).
- **Visual Monitoring:** Integrated Apache Kafka UI dashboard to inspect brokers, topics, consumer groups, and active connector status.

---

## 🚀 Quickstart

### 1. Launch Infrastructure via Docker Compose

Ensure Docker Desktop is running, then spin up the complete event streaming platform:

```bash
# Build custom images (Kafka Connect with S3 plugin & Producer) and start all services
make build
```
or
```bash
docker compose -f .docker/docker-compose.yaml up -d --build
```

### 2. Service Access Points

| Service | Port | Default Credentials / Endpoint | Description |
| :--- | :--- | :--- | :--- |
| **FastAPI Producer API** | `8000` | `http://localhost:8000/docs` | Webhook API & Event Simulator |
| **Kafka UI** | `8088` | `http://localhost:8088` | Cluster, Topic & Connector GUI |
| **Kafka Connect REST API** | `8083` | `http://localhost:8083/connectors` | Kafka Connect Management API |
| **MinIO S3 Console** | `9001` | `minioadmin` / `minioadmin` | S3 Data Lake Storage Console |
| **MinIO S3 API** | `9000` | `http://localhost:9000` | S3 Endpoint for S3 Connectors & Engines |
| **Apache Kafka Broker** | `9092` | `localhost:9092` (Host) / `kafka:29092` | KRaft Broker & Bootstrap Server |

---

## 💻 Local Development & Event Producer

The event producer is written in Python 3.10 using `FastAPI` and `confluent-kafka`. It runs inside Docker but supports local execution and API triggering.

### 1. Trigger Event Generator via API

Start generating simulated CRM events (batches of 15–30 events pushed every 3 seconds):

```bash
# Start background simulation
curl -X POST "http://localhost:8000/simulation/start?interval_seconds=3"

# Check simulation status
curl -X GET "http://localhost:8000/simulation/status"

# Stop background simulation
curl -X POST "http://localhost:8000/simulation/stop"
```

### 2. Ingest Manual Webhooks

Send custom webhooks to target topics (`crm-contacts`, `crm-leads`, `crm-deals`, `crm-engagements`):

```bash
curl -X POST "http://localhost:8000/webhooks/crm/contacts" \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "contact.created",
    "object_id": "con_998877",
    "data": {
      "first_name": "Alice",
      "last_name": "Smith",
      "email": "alice@example.com",
      "lifecycle_stage": "customer"
    }
  }'
```

---

## 📡 Kafka Connect & MinIO Storage Layout

Kafka Connect automatically registers the `s3-sink-crm` connector on container initialization (`.docker/kafka-connect/init-connectors.sh`).

### Connector Properties (`s3-sink-crm.json`)
- **Topic Pattern:** Matches all topics beginning with `crm-.*`
- **Target Bucket:** `kafka-s3-events-sink`
- **Format:** JSON (`JsonFormat`)
- **Partitioner:** `TimeBasedPartitioner` (Hourly partitions in UTC)
- **Flush Threshold:** 15 records or 60,000 ms (1 minute)

### S3 Directory Structure
```text
s3://kafka-s3-events-sink/
├── crm-contacts/year=2026/month=08/day=23/hour=23/crm-contacts+0+0000000000.json
├── crm-leads/year=2026/month=08/day=23/hour=23/crm-leads+0+0000000000.json
├── crm-deals/year=2026/month=08/day=23/hour=23/crm-deals+0+0000000000.json
└── crm-engagements/year=2026/month=08/day=23/hour=23/crm-engagements+0+0000000000.json
```

---

## 🔮 Future Roadmap & TODOs

> [!IMPORTANT]
> **Planned Platform Enhancements (from [`TODOS.md`](file:///C:/Users/alias/repos/python-kafka-s3-event-pipeline/TODOS.md)):**
> - **Change Data Capture (CDC):** Add PostgreSQL service + Debezium Source Connector to stream binlog database mutations to S3.
> - **Secrets Management:** Integrate HashiCorp Vault service for container secret resolution.
> - **Connector UI:** Add specialized management UI for interactive Kafka Connector management.

---

## 📂 Repository Structure

```text
python-kafka-s3-event-pipeline/
├── .docker/
│   ├── docker-compose.yaml                    # Infrastructure definition (Kafka, Connect, MinIO, UI, Producer)
│   ├── .env                                   # Shared environment variables
│   ├── kafka/                                 # KRaft Kafka broker environment configuration
│   ├── kafka-connect/                         # Custom Dockerfile & connector auto-registration script
│   ├── kafka-ui/                              # Kafka UI environment configuration
│   └── producer/                              # Python producer Dockerfile & requirements
├── kafka-connect/
│   └── connectors/
│       └── s3-sink-crm.json                   # S3 Sink connector JSON specification
├── producer/
│   └── app/
│       ├── main.py                            # FastAPI application entrypoint & webhook routes
│       ├── generator.py                       # Async background event generator worker
│       ├── kafka_client.py                    # Wrapper for confluent-kafka Producer
│       └── mock_data.py                       # Event schema envelope & Faker data generators
├── ARCHITECTURE.md                            # Detailed technical architecture & data lifecycle
├── Makefile                                   # CLI helper commands for container management
├── README.md                                  # Repository overview & quickstart guide
└── TODOS.md                                   # Backlog of architectural improvements
```
