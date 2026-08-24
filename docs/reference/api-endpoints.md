# 🌐 API Endpoints Reference

The **CRM Webhook Producer API** provides HTTP endpoints for manual webhook ingestion and automated background CRM event generation.

- **Base URL:** `http://localhost:8000`
- **Versioned Base Path:** `/api/v1`
- **Swagger Documentation:** `http://localhost:8000/docs`
- **ReDoc Documentation:** `http://localhost:8000/redoc`

---

## 1. Webhook Ingestion Endpoints

### Ingest CRM Webhook
Receives a raw CRM webhook payload, wraps it into a standardized event envelope, and delivers it asynchronously to the appropriate Kafka topic (`crm-<object_type>`).

- **Route:** `POST /api/v1/webhooks/crm/{object_type}` *(Alias: `POST /webhooks/crm/{object_type}`)*
- **Path Parameters:**
  - `object_type` (`string`, required): One of `contacts`, `leads`, `deals`, `engagements`.
- **Status Code:** `202 Accepted`

#### Request Payload:
```json
{
  "event_type": "contact.created",
  "object_id": "con_998877",
  "data": {
    "first_name": "Alice",
    "last_name": "Smith",
    "email": "alice@example.com",
    "lifecycle_stage": "customer"
  }
}
```

#### Successful Response (`202 Accepted`):
```json
{
  "status": "accepted",
  "topic": "crm-contacts",
  "object_id": "con_998877"
}
```

#### cURL Example:
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

---

## 2. Event Simulation Endpoints

### Start Background Simulation
Starts an asynchronous background loop generating random batches of 15–30 CRM events pushed to Kafka at configured intervals.

- **Route:** `POST /api/v1/simulation/start` *(Alias: `POST /simulation/start`)*
- **Query Parameters:**
  - `interval_seconds` (`integer`, optional, default: `3`, min: `1`, max: `60`): Seconds between event generation ticks.
- **Status Code:** `200 OK`

#### Response:
```json
{
  "status": "started",
  "interval_seconds": 3
}
```
*(If already active: `{"status": "already_running"}`)*

#### cURL Example:
```bash
curl -X POST "http://localhost:8000/api/v1/simulation/start?interval_seconds=3"
```

---

### Stop Background Simulation
Cancels the background async loop.

- **Route:** `POST /api/v1/simulation/stop` *(Alias: `POST /simulation/stop`)*
- **Status Code:** `200 OK`

#### Response:
```json
{
  "status": "stopped"
}
```

#### cURL Example:
```bash
curl -X POST "http://localhost:8000/api/v1/simulation/stop"
```

---

### Get Simulation Status
Checks if the background generator loop is currently running.

- **Route:** `GET /api/v1/simulation/status` *(Alias: `GET /simulation/status`)*
- **Status Code:** `200 OK`

#### Response:
```json
{
  "is_running": true
}
```

#### cURL Example:
```bash
curl -X GET "http://localhost:8000/api/v1/simulation/status"
```

---

## 3. Healthcheck Endpoint

- **Route:** `GET /health`
- **Status Code:** `200 OK`

#### Response:
```json
{
  "status": "ok",
  "version": "1.0.0"
}
```
