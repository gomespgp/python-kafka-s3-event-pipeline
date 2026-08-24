# 🩺 Troubleshooting Guide

Common issues, diagnostic checks, and solutions when running the Kafka-to-S3 event pipeline.

---

## 1. Kafka Connect Fails to Start or Shows Unhealthy

### Symptoms:
- `kafka-connect` container exits or healthcheck fails (`curl -s http://localhost:8083/connectors`).

### Checks & Solutions:
1. **Wait for Kafka Broker Health:**
   Kafka Connect depends on `kafka` being fully healthy. Check Kafka broker logs:
   ```bash
   docker compose -f .docker/docker-compose.yaml logs kafka
   ```
2. **Inspect S3 Connector Plugin Installation:**
   Verify the S3 plugin was successfully installed during Docker build:
   ```bash
   docker compose -f .docker/docker-compose.yaml exec kafka-connect confluent-hub list
   ```
   *Expected output:* `confluentinc/kafka-connect-s3:10.5.13`

---

## 2. Connector Registered but No Files Appearing in MinIO

### Symptoms:
- Events appear in Kafka topics on Kafka UI (`:8088`), but bucket `kafka-s3-events-sink` remains empty in MinIO.

### Checks & Solutions:
1. **Check Flush Size & Rotation Interval:**
   The connector buffers records in memory until either:
   - `flush.size: 15` records are reached, **OR**
   - `rotate.interval.ms: 60000` (1 minute) elapses.
   If you only sent 1 or 2 test events, wait 60 seconds or send 15+ events to trigger a flush.
2. **Inspect Connector Status & Task Errors:**
   ```bash
   curl -s http://localhost:8083/connectors/s3-sink-crm/status | jq .
   ```
   Check if `state` is `RUNNING` or `FAILED`. If `FAILED`, check the `trace` field for error details.
3. **Verify MinIO S3 Bucket Exists:**
   Ensure the `minio-init` container ran and created `kafka-s3-events-sink`:
   ```bash
   docker compose -f .docker/docker-compose.yaml logs minio-init
   ```

---

## 3. Producer Cannot Connect to Kafka

### Symptoms:
- Producer logs show `Message delivery failed` or broker connection timeouts.

### Checks & Solutions:
- **Inside Docker:** The producer must connect to `kafka:29092` (using Docker DNS and internal listener).
- **Outside Docker (Local host):** The producer must connect to `localhost:9092` (using the host-advertised listener).

---

## 4. Resetting Cluster to a Clean Slate

If topics, offsets, or corrupted state need a complete wipe:

```bash
# Stop all containers and wipe persistent Docker volumes
make clean

# Rebuild and start fresh
make build
```
