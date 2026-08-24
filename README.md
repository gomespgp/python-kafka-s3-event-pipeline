# ⚡ Python Kafka S3 Event Pipeline

An end-to-end real-time event streaming and ingestion platform leveraging Apache Kafka (KRaft mode), Python (FastAPI + `confluent-kafka`), Kafka Connect S3 Sink, and MinIO object storage to stream CRM webhooks and events into a partitioned Data Lake S3 landing zone.

---

## 📌 Features

- **Real-Time Streaming:** High-throughput event ingestion using Confluent Apache Kafka in ZooKeeper-less KRaft mode.
- **Modular FastAPI Producer & Simulator:** Layered application architecture with versioned API endpoints, dependency injection, Pydantic data schemas, and background simulation workers.
- **Automated S3 Sink Connector:** Confluent Kafka Connect S3 Sink automatically registered on startup via a lightweight container sidecar.
- **Time-Based Partitioning:** Dynamic object storage directory layout in MinIO (`kafka-s3-events-sink/crm-<type>/year=YYYY/month=MM/day=DD/hour=HH/`).
- **Visual Monitoring:** Integrated Apache Kafka UI dashboard to inspect brokers, topics, consumer groups, and active connector status.
- **Automated Testing & CI:** Pytest unit/API test suite with GitHub Actions CI pipeline running on every pull request to `master`.

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

## 💻 Quick cURL Triggers

### 1. Trigger Background Event Generator

```bash
# Start background simulation (15–30 events every 3s)
curl -X POST "http://localhost:8000/api/v1/simulation/start?interval_seconds=3"

# Check simulation status
curl -X GET "http://localhost:8000/api/v1/simulation/status"

# Stop simulation
curl -X POST "http://localhost:8000/api/v1/simulation/stop"
```

### 2. Ingest Manual Webhook

```bash
curl -X POST "http://localhost:8000/api/v1/webhooks/crm/contacts" \
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

### 3. Run Automated Tests

```bash
# Run unit and API tests
make test
```

---

## 📚 Documentation Knowledge Base

For comprehensive technical design, configurations, and developer guides, explore the [`docs/`](docs/README.md) directory:

- 🏛️ **[System Architecture Overview](docs/architecture/overview.md)** — Topology diagrams and service interaction flows.
- ⚙️ **[Kafka Broker & Connect Mechanics](docs/architecture/kafka-broker-connect.md)** — KRaft consensus, delivery guarantees, and S3 Sink buffering.
- 🔄 **[Data Lifecycle & Partitioning](docs/architecture/data-lifecycle.md)** — Event schemas, payload envelopes, and S3 directory layouts.
- 📋 **[Configuration Reference](docs/reference/configuration.md)** — Detailed breakdown of `.env` variables and connector JSON.
- 🌐 **[API Reference](docs/reference/api-endpoints.md)** — Complete OpenAPI/cURL specification for all endpoints.
- 🛠️ **[Local Development Guide](docs/guides/local-development.md)** — Makefile commands and local workflow.
- 🩺 **[Troubleshooting Guide](docs/guides/troubleshooting.md)** — Common connector, broker, and MinIO fixes.

---

## 🔮 Future Roadmap & TODOs

> [!IMPORTANT]
> **Planned Platform Enhancements (from [`TODOS.md`](file:///C:/Users/alias/repos/python-kafka-s3-event-pipeline/TODOS.md)):**
> - **Change Data Capture (CDC):** Add PostgreSQL service + Debezium Source Connector to stream binlog database mutations to S3.
> - **Medallion Architecture:** Expand from Bronze JSON landing into Silver and Gold analytical layers with DuckDB / dbt.
> - **Kubernetes & Strimzi:** Deploy FastAPI Producer via K8s manifests (Deployments/HPA) and manage Kafka declaratively using the Strimzi Operator.
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
│   └── producer/                              # Python producer Dockerfile & requirements.txt
├── .github/
│   └── workflows/
│       └── producer-test.yaml                 # GitHub Actions CI workflow (triggers on PRs to master)
├── docs/                                      # Modular documentation knowledge base
│   ├── README.md                              # Documentation Hub & Sitemap
│   ├── architecture/                          # Architecture flowcharts & deep dives
│   │   ├── overview.md
│   │   ├── kafka-broker-connect.md
│   │   └── data-lifecycle.md
│   ├── reference/                             # Reference manuals
│   │   ├── configuration.md
│   │   └── api-endpoints.md
│   └── guides/                                # Practical developer guides
│       ├── local-development.md
│       └── troubleshooting.md
├── kafka-connect/
│   └── connectors/
│       └── s3-sink-crm.json                   # S3 Sink connector JSON specification
├── producer/
│   ├── app/
│   │   ├── main.py                            # FastAPI application factory & lifespan context
│   │   ├── api/                               # HTTP layer & APIRouter endpoints
│   │   │   └── v1/
│   │   │       ├── router.py                  # API router aggregator
│   │   │       └── endpoints/
│   │   │           ├── simulation.py          # /simulation endpoints
│   │   │           └── webhooks.py            # /webhooks endpoints
│   │   ├── core/                              # Infrastructure singletons & configuration
│   │   │   ├── config.py                      # Pydantic BaseSettings (.env loader)
│   │   │   └── kafka.py                       # Kafka Producer client & dependency injection
│   │   ├── schemas/                           # Pydantic domain models & contracts
│   │   │   ├── events.py                      # WebhookPayload, EventEnvelope, Entity enums
│   │   │   └── responses.py                   # API response models
│   │   └── services/                          # Business logic & background workers
│   │       ├── generator.py                   # Asyncio background event simulator loop
│   │       └── mock_factory.py                # Faker mock CRM event generators
│   └── tests/                                 # Automated Pytest suite
│       ├── conftest.py                        # TestClient & mock Kafka fixtures
│       ├── api/                               # API route tests (webhooks, simulation, health)
│       └── unit/                              # Unit tests (schemas, config, mock generator)
├── ARCHITECTURE.md                            # High-level architecture summary & links
├── Makefile                                   # CLI helper commands for container management
├── pyproject.toml                             # Project metadata, dev dependencies & pytest configuration
├── README.md                                  # Repository overview & quickstart guide
└── TODOS.md                                   # Backlog of architectural improvements
```
