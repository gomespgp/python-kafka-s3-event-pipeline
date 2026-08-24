# 🔄 Data Lifecycle & Partitioning

This document outlines the event payload data contract, topic routing rules, and S3 object storage partitioning hierarchy.

---

## 1. Event Payload Envelope Specification

Every CRM event published to Kafka is wrapped in a consistent JSON envelope schema defined in `producer/app/schemas/events.py`:

```json
{
  "event_id": "evt_a1b2c3d4e5f6",
  "event_type": "contact.created",
  "object_type": "contacts",
  "object_id": "con_1a2b3c4d",
  "timestamp": "2026-08-24T21:30:00.000000+00:00",
  "data": {
    "first_name": "Jane",
    "last_name": "Doe",
    "email": "jane.doe@example.com",
    "lifecycle_stage": "opportunity"
  }
}
```

### Envelope Field Definitions:
- **`event_id`** (`string`): Unique UUID identifying this single mutation event.
- **`event_type`** (`string`): Specific action (e.g. `deal.stage_updated`, `lead.qualified`).
- **`object_type`** (`string`): Entity category (`contacts`, `leads`, `deals`, `engagements`).
- **`object_id`** (`string`): Unique identifier of the entity in the source CRM. Used as the Kafka **message key** to ensure partition ordering.
- **`timestamp`** (`string`): UTC ISO 8601 timestamp when the event occurred.
- **`data`** (`object`): Entity-specific attributes and payload data.

---

## 2. Supported Entities & Topic Mapping

| Object Type (`object_type`) | Target Kafka Topic | Supported Actions (`event_type`) | Core Payload Attributes (`data`) |
| :--- | :--- | :--- | :--- |
| `contacts` | `crm-contacts` | `contact.created`, `contact.updated`, `contact.lifecycle_changed` | `first_name`, `last_name`, `email`, `lifecycle_stage` |
| `leads` | `crm-leads` | `lead.created`, `lead.qualified`, `lead.status_updated` | `lead_score`, `source`, `status` |
| `deals` | `crm-deals` | `deal.created`, `deal.stage_updated`, `deal.closed_won`, `deal.closed_lost` | `deal_name`, `amount`, `currency`, `stage` |
| `engagements` | `crm-engagements` | `email.sent`, `call.completed`, `meeting.scheduled` | `channel`, `rep_email`, `duration_seconds`, `notes` |

---

## 3. S3 Storage Hierarchy & Hive Partitioning

Kafka Connect writes newline-delimited JSON (`.json`) files directly to MinIO bucket `kafka-s3-events-sink` following **Hive-compatible directory paths**:

```text
s3://kafka-s3-events-sink/
├── crm-contacts/
│   └── year=2026/
│       └── month=08/
│           └── day=24/
│               └── hour=21/
│                   └── crm-contacts+0+0000000000.json
├── crm-leads/
│   └── year=2026/month=08/day=24/hour=21/crm-leads+0+0000000000.json
├── crm-deals/
│   └── year=2026/month=08/day=24/hour=21/crm-deals+0+0000000000.json
└── crm-engagements/
    └── year=2026/month=08/day=24/hour=21/crm-engagements+0+0000000000.json
```

### S3 File Naming Convention:
`[topic_name]+[kafka_partition_id]+[starting_offset].json`

- **Example:** `crm-contacts+0+0000000045.json`
- **Idempotency Guarantee:** If a connector task crashes and re-processes records from offset 45, it writes to the exact same file name, guaranteeing that no duplicate files or ghost data appear in S3.
