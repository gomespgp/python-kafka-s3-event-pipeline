# 💻 Local Development & Makefile Guide

This guide explains how to set up, run, test, and manage the Python Kafka S3 Event Pipeline platform locally.

---

## 1. Prerequisites

- **Docker Desktop** (running with Compose v2)
- **Make** (installed via Git Bash, Chocolatey, or WSL)
- **Python 3.10+** (for local development outside containers)

---

## 2. Makefile Lifecycle Commands

The repository includes a root [`Makefile`](file:///C:/Users/alias/repos/python-kafka-s3-event-pipeline/Makefile) to streamline container lifecycle and testing management:

```bash
# Build custom Docker images and start all services in the background
make build

# Start services (if already built)
make up

# View real-time logs for all running services
make logs

# Restart a specific service (e.g. kafka-connect or producer)
make restart service=kafka-connect

# Run automated unit and API test suite with Pytest
make test

# Stop and remove containers and networks
make down

# Clean up all containers, networks, AND persistent volumes (Fresh Start)
make clean
```

---

## 3. Running the Python Producer Locally (Outside Docker)

If you wish to develop and debug the FastAPI producer locally while Kafka and MinIO run in Docker:

1. **Activate Virtual Environment:**
   ```powershell
   .venv\Scripts\Activate.ps1
   ```
2. **Install Local Dependencies & Dev Tools:**
   ```powershell
   pip install -e ".[dev]"
   ```
3. **Run with Uvicorn:**
   ```powershell
   $env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
   uvicorn app.main:app --app-dir producer --reload --port 8000
   ```
4. **Access Swagger UI:** Navigate to `http://localhost:8000/docs`.

---

## 4. Running the Automated Test Suite

Run unit and route integration tests locally:

```powershell
# Using Make
make test

# Or directly via Pytest
pytest producer/tests -v
```

> **Note:** Pytest is configured via `pyproject.toml` with `-p no:cacheprovider` to prevent `.pytest_cache` folders from being written to disk.

---

## 5. Validating the Pipeline End-to-End

1. **Check Services Health:**
   ```bash
   docker compose -f .docker/docker-compose.yaml ps
   ```
2. **Start Event Simulation:**
   ```bash
   curl -X POST "http://localhost:8000/api/v1/simulation/start?interval_seconds=2"
   ```
3. **Inspect Topics in Kafka UI:**
   Open `http://localhost:8088` and verify messages are arriving on `crm-contacts`, `crm-leads`, `crm-deals`, and `crm-engagements`.
4. **Verify Files in MinIO S3 Console:**
   Open `http://localhost:9001` (Login: `minioadmin` / `minioadmin`), browse bucket `kafka-s3-events-sink`, and observe time-partitioned JSON files being created.
