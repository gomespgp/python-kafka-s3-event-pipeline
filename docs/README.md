# 📖 Documentation Hub

Welcome to the **Python Kafka S3 Event Pipeline** documentation knowledge base. Use the links below to navigate through technical architecture designs, configuration references, and developer guides.

---

## 🗺️ Documentation Sitemap

### 🏛️ 1. Architecture & Design
Detailed technical design, message flowcharts, and system topology.

- **[System Overview](architecture/overview.md)** — High-level architecture, infrastructure ports, and service dependency graph.
- **[Kafka Broker & Connector Engine Guide](architecture/kafka-broker-connect.md)** — Deep dive on KRaft consensus, listener networking, and S3 Sink connector mechanics.
- **[Data Lifecycle & Partitioning](architecture/data-lifecycle.md)** — Event payload envelope schemas, topic mapping, and S3 Hive directory layout.

---

### 📚 2. Reference Manuals
Exact configuration variables and API specifications.

- **[Configuration Reference](reference/configuration.md)** — Variable-by-variable documentation for all `.env` files and `s3-sink-crm.json`.
- **[API Endpoints Reference](reference/api-endpoints.md)** — Complete OpenAPI/cURL specification for Webhook ingestion and Simulator control endpoints.

---

### 🛠️ 3. Guides & Operations
Practical how-to guides for running, testing, and troubleshooting.

- **[Local Development Guide](guides/local-development.md)** — How to run via Makefile, develop locally with virtualenv, and test end-to-end.
- **[Troubleshooting Guide](guides/troubleshooting.md)** — Diagnosing Kafka Connect issues, buffer flush behavior, and connection troubleshooting.

---

## ⚡ Quick Navigation

| Document | Primary Audience | Key Topics |
| :--- | :--- | :--- |
| **[Architecture Overview](architecture/overview.md)** | Architects & Leads | System topology, Mermaid diagrams, service interactions |
| **[Broker & Connect Guide](architecture/kafka-broker-connect.md)** | Data Engineers | KRaft mode, partition keys, delivery callbacks, flush triggers |
| **[Configuration Reference](reference/configuration.md)** | DevOps / SRE | Environment variables, ports, credentials, JSON parameters |
| **[API Reference](reference/api-endpoints.md)** | Application Developers | Webhooks, request/response models, simulation control |
| **[Troubleshooting Guide](guides/troubleshooting.md)** | Operations | Debugging failed connectors, MinIO storage verification |
