# ⚙️ Configuration Reference Guide

This document provides a detailed reference for every environment variable and configuration parameter across all services in the Python Kafka S3 Event Pipeline platform.

---

## 1. 🏗️ Apache Kafka Broker — `.docker/kafka/.env`

Confluent Kafka in KRaft mode is configured entirely via environment variables. These map directly to `server.properties` settings inside the container.

```properties
KAFKA_NODE_ID=1
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT
KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
KAFKA_PROCESS_ROLES=broker,controller
KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:29093
KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093
KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT
KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
KAFKA_LOG_DIRS=/tmp/kraft-combined-logs
CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk
```

### KRaft Identity & Roles

| Variable | Value | Explanation |
| :--- | :--- | :--- |
| `KAFKA_NODE_ID` | `1` | Unique numeric ID for this Kafka node within the cluster. In a multi-broker cluster, each node would have a different ID (1, 2, 3...). |
| `KAFKA_PROCESS_ROLES` | `broker,controller` | Defines what roles this node plays. `broker` handles client produce/consume requests. `controller` manages cluster metadata (topics, partitions, ISR). In KRaft mode, one node can serve both roles. In production, these would be separate nodes. |
| `CLUSTER_ID` | `MkU3OEVBNTcwNTJENDM2Qk` | A stable base64-encoded UUID that uniquely identifies this Kafka cluster. Must be generated once (via `kafka-storage random-uuid`) and remain fixed across restarts — it's written into the KRaft metadata log on first boot. |

### Listener Architecture

The listener configuration is one of the most complex parts of Kafka. There are three separate concepts that work together:

```text
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: defines which protocol each named listener uses
KAFKA_LISTENERS:                       defines which ports/interfaces this broker binds to
KAFKA_ADVERTISED_LISTENERS:            defines what addresses clients are told to connect to
```

| Variable | Value | Explanation |
| :--- | :--- | :--- |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | `PLAINTEXT:PLAINTEXT, PLAINTEXT_HOST:PLAINTEXT, CONTROLLER:PLAINTEXT` | Maps a **listener name** to a **security protocol**. `PLAINTEXT` means no TLS/SSL. In production you would use `SSL` or `SASL_SSL`. The three names are: `PLAINTEXT` (internal inter-broker), `PLAINTEXT_HOST` (external host access), `CONTROLLER` (metadata quorum). |
| `KAFKA_LISTENERS` | `PLAINTEXT://0.0.0.0:29092, PLAINTEXT_HOST://0.0.0.0:9092, CONTROLLER://0.0.0.0:29093` | The actual **network interfaces and ports** Kafka binds to inside the container. `0.0.0.0` means accept connections on any network interface. Three ports serve three purposes: `29092` (Docker internal), `9092` (exposed to host), `29093` (controller quorum traffic). |
| `KAFKA_ADVERTISED_LISTENERS` | `PLAINTEXT://kafka:29092, PLAINTEXT_HOST://localhost:9092` | After a client connects, Kafka sends back these **advertised addresses** as the actual endpoints to use. `kafka:29092` is for other containers inside the Docker network (using Docker DNS). `localhost:9092` is for clients running directly on your machine. The `CONTROLLER` listener is intentionally excluded here — it is for internal Raft consensus only, not client traffic. |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | `PLAINTEXT` | In a multi-broker cluster, brokers replicate data between each other using this named listener. Set to `PLAINTEXT` (port `29092`) so broker-to-broker traffic stays on the Docker internal network. |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | `CONTROLLER` | Tells Kafka which listener name is dedicated to KRaft controller quorum communication. Isolates metadata traffic from data traffic. |

### Internal Topic Replication

| Variable | Value | Explanation |
| :--- | :--- | :--- |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `1` | Kafka stores consumer group offset commits in an internal topic (`__consumer_offsets`). This controls how many brokers replicate that topic. Set to `1` because we only have one broker. **In production with 3+ brokers, set to `3`.** |
| `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` | `1` | Same as above but for the internal transaction state topic (`__transaction_state`). Required for exactly-once semantics. Set to `1` for single-broker local setup. |
| `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR` | `1` | ISR = **In-Sync Replicas**. A transaction is only committed when at least this many replicas have acknowledged the write. Must be ≤ replication factor. Set to `1` for single-broker. **In production, typically set to `2`.** |

### Performance & Startup

| Variable | Value | Explanation |
| :--- | :--- | :--- |
| `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` | `0` | When a consumer group starts, Kafka waits this many milliseconds before triggering the first partition rebalance, hoping more consumers will join. Set to `0` to disable the wait and make local startup faster. **In production, set to `3000` (3 seconds).** |
| `KAFKA_LOG_DIRS` | `/tmp/kraft-combined-logs` | Directory inside the container where Kafka stores partition log segments and KRaft metadata. Using `/tmp` means data is **ephemeral** — it's lost when the container restarts. For persistence, mount a Docker volume here. |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@kafka:29093` | Defines the full list of KRaft controller nodes eligible to participate in leader election. Format: `{node_id}@{host}:{port}`. In a production 3-controller cluster this would be: `1@kafka-1:29093,2@kafka-2:29093,3@kafka-3:29093`. |

---

## 2. 🔌 Kafka Connect Engine — `.docker/kafka-connect/.env`

Kafka Connect's worker configuration controls how the distributed runtime itself operates — its internal storage topics, serialization, and plugin paths.

```properties
CONNECT_BOOTSTRAP_SERVERS=kafka:29092
CONNECT_REST_PORT=8083
CONNECT_GROUP_ID=kafka-connect-s3-group
CONNECT_CONFIG_STORAGE_TOPIC=docker-connect-configs
CONNECT_OFFSET_STORAGE_TOPIC=docker-connect-offsets
CONNECT_STATUS_STORAGE_TOPIC=docker-connect-status
CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1
CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1
CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1
CONNECT_KEY_CONVERTER=org.apache.kafka.connect.storage.StringConverter
CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter
CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE=false
CONNECT_REST_ADVERTISED_HOST_NAME=kafka-connect
CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components
AWS_ACCESS_KEY_ID=${MINIO_ROOT_USER}
AWS_SECRET_ACCESS_KEY=${MINIO_ROOT_PASSWORD}
```

### Cluster & REST API

| Variable | Value | Explanation |
| :--- | :--- | :--- |
| `CONNECT_BOOTSTRAP_SERVERS` | `kafka:29092` | The Kafka broker address Kafka Connect uses to connect as a **client**. Uses the internal Docker DNS name `kafka` and the internal listener port `29092`. |
| `CONNECT_REST_PORT` | `8083` | Port where Kafka Connect exposes its **REST Management API** for registering, pausing, deleting, and inspecting connectors. This is the port exposed to the host in `docker-compose.yaml`. |
| `CONNECT_REST_ADVERTISED_HOST_NAME` | `kafka-connect` | The hostname Kafka Connect advertises to other Connect workers in a distributed cluster. Other containers (like `kafka-connect-init`) use this hostname to reach the REST API. |
| `CONNECT_GROUP_ID` | `kafka-connect-s3-group` | Consumer group ID used by all workers in this Connect cluster. Workers sharing the same group ID form a **distributed Connect cluster**, automatically sharing connector tasks between them. |

### Internal State Storage Topics

Kafka Connect stores its own state **back into Kafka** using three dedicated internal topics. This is what makes Connect fault-tolerant: any worker can crash and another worker picks up from exactly where it left off.

| Variable | Value | Explanation |
| :--- | :--- | :--- |
| `CONNECT_CONFIG_STORAGE_TOPIC` | `docker-connect-configs` | Stores the JSON configuration of all registered connectors. When you `PUT /connectors/s3-sink-crm/config`, the config is written here. All workers read from this topic to know what connectors to run. |
| `CONNECT_OFFSET_STORAGE_TOPIC` | `docker-connect-offsets` | Stores the last committed **Kafka offset** for each connector task. This is how Connect resumes from exactly the right position after a crash or restart — without re-processing old records or skipping new ones. |
| `CONNECT_STATUS_STORAGE_TOPIC` | `docker-connect-status` | Stores the running status (`RUNNING`, `PAUSED`, `FAILED`) of each connector and its individual tasks. This is what the Kafka Connect REST API reads to answer `GET /connectors/s3-sink-crm/status`. |
| `CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR` | `1` | How many brokers replicate each internal topic. Set to `1` for single-broker local setup. **In production: `3`.** |
| `CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR` | `1` | Same as above but for the offset storage topic. |
| `CONNECT_STATUS_STORAGE_REPLICATION_FACTOR` | `1` | Same as above but for the status storage topic. |

### Serialization (Converters)

Converters control how Kafka Connect **deserializes records from Kafka** before passing them to connector tasks for processing.

| Variable | Value | Explanation |
| :--- | :--- | :--- |
| `CONNECT_KEY_CONVERTER` | `org.apache.kafka.connect.storage.StringConverter` | Deserializes the Kafka message **key** as a plain UTF-8 string. Matches how Python sends keys (`object_id.encode("utf-8")`). |
| `CONNECT_VALUE_CONVERTER` | `org.apache.kafka.connect.json.JsonConverter` | Deserializes the Kafka message **value** as a JSON object. Matches how Python serializes payloads (`json.dumps(payload).encode("utf-8")`). |
| `CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE` | `false` | When `true`, the JSON converter expects a Confluent Schema Registry-compatible envelope `{"schema": {...}, "payload": {...}}`. Set to `false` because we publish raw JSON objects without schema headers. |

### Plugins & Credentials

| Variable | Value | Explanation |
| :--- | :--- | :--- |
| `CONNECT_PLUGIN_PATH` | `/usr/share/java, /usr/share/confluent-hub-components` | Comma-separated list of directories where Kafka Connect scans for connector JARs on startup. The S3 Sink JAR installed via `confluent-hub install` lands in `/usr/share/confluent-hub-components`. |
| `AWS_ACCESS_KEY_ID` | `${MINIO_ROOT_USER}` | AWS SDK credential injected as an OS environment variable. The S3 Sink Connector uses the AWS Java SDK to communicate with the S3 API, and this is how it authenticates. The value is interpolated from the shared `.env` (`minioadmin`). |
| `AWS_SECRET_ACCESS_KEY` | `${MINIO_ROOT_PASSWORD}` | AWS SDK secret credential, also interpolated from the shared `.env` (`minioadmin`). MinIO accepts these exactly as if they were real AWS credentials. |

---

## 3. 🖥️ Kafka UI — `.docker/kafka-ui/.env`

Provectus Kafka UI connects to the broker and Kafka Connect to provide a web dashboard.

```properties
KAFKA_CLUSTERS_0_NAME=local-cluster
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:29092
KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME=local-connect
KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS=http://kafka-connect:8083
```

| Variable | Value | Explanation |
| :--- | :--- | :--- |
| `KAFKA_CLUSTERS_0_NAME` | `local-cluster` | Human-readable display name for this cluster in the Kafka UI dashboard. The `_0_` index allows defining multiple clusters (e.g. `KAFKA_CLUSTERS_1_NAME=production-cluster`). |
| `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS` | `kafka:29092` | The broker address Kafka UI connects to as a metadata client. Uses the Docker-internal listener. |
| `KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME` | `local-connect` | Display name for the Kafka Connect instance shown in the UI under the "Kafka Connect" tab. |
| `KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS` | `http://kafka-connect:8083` | REST API URL Kafka UI queries to fetch connector list, status, and task metrics. The `_0_` index allows registering multiple Connect clusters per Kafka cluster. |

---

## 4. 🐍 Python Producer — `.docker/producer/.env`

```properties
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
```

| Variable | Value | Explanation |
| :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | The broker address the Python `confluent-kafka` `Producer` class connects to when the container starts. Uses the Docker-internal DNS name and internal listener port. If running the Python app locally outside Docker, this would be `localhost:9092`. |

---

## 5. 📄 S3 Sink Connector — `kafka-connect/connectors/s3-sink-crm.json`

The connector JSON is the declarative specification for a single **connector instance** registered into the Kafka Connect runtime via the REST API.

```json
{
  "name": "s3-sink-crm",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "1",
    "topics.regex": "crm-.*",

    "s3.region": "us-east-1",
    "s3.bucket.name": "kafka-s3-events-sink",
    "s3.part.size": "5242880",
    "store.url": "http://minio:9000",
    "aws.access.key.id": "minioadmin",
    "aws.secret.access.key": "minioadmin",

    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "format.class": "io.confluent.connect.s3.format.json.JsonFormat",

    "flush.size": "15",
    "rotate.interval.ms": "60000",

    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
    "partition.duration.ms": "3600000",
    "timestamp.extractor": "Wallclock",
    "locale": "en-US",
    "timezone": "UTC",

    "schema.compatibility": "NONE",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"
  }
}
```

### Identity & Parallelism

| Parameter | Value | Explanation |
| :--- | :--- | :--- |
| `name` | `s3-sink-crm` | Unique name of this connector instance within the Connect cluster. Used as the resource identifier in all REST API calls (`PUT /connectors/s3-sink-crm/config`, `GET /connectors/s3-sink-crm/status`). |
| `connector.class` | `io.confluent.connect.s3.S3SinkConnector` | Fully qualified Java class name of the connector plugin to load. Kafka Connect scans `CONNECT_PLUGIN_PATH` for this class at startup. |
| `tasks.max` | `1` | Maximum number of **parallel worker tasks** Kafka Connect may spin up for this connector. Each task handles a subset of the matched topic partitions independently. Set to `1` because we have a single partition per topic locally. **In production with many partitions, increase to match partition count.** |

### Topic Selection

| Parameter | Value | Explanation |
| :--- | :--- | :--- |
| `topics.regex` | `crm-.*` | Java regex pattern matched against all topic names in the broker. Any topic whose name starts with `crm-` is automatically assigned to this connector — including topics created after the connector was registered. An alternative is `topics: "crm-contacts,crm-leads"` for an explicit comma-separated list. |

### S3 / MinIO Storage Target

| Parameter | Value | Explanation |
| :--- | :--- | :--- |
| `store.url` | `http://minio:9000` | Overrides the default AWS S3 endpoint with the local MinIO S3-compatible API address. Without this, the connector would try to reach `s3.amazonaws.com`. |
| `s3.bucket.name` | `kafka-s3-events-sink` | Target bucket where all topic data lands. Must already exist when the connector starts — created by the `minio-init` container. |
| `s3.region` | `us-east-1` | AWS region passed to the S3 SDK. MinIO ignores this value but it is required by the AWS SDK for signature construction. Can be any valid region string. |
| `s3.part.size` | `5242880` | Minimum size in bytes (5 MB) for S3 multipart upload parts. Files smaller than this are uploaded as a single PUT. Files larger use multipart upload for efficiency and fault tolerance. |
| `aws.access.key.id` | `minioadmin` | S3 credential — duplicated here at the connector level in addition to the worker-level environment variable for explicit per-connector credential scoping. |
| `aws.secret.access.key` | `minioadmin` | S3 secret credential at the connector level. |
| `storage.class` | `io.confluent.connect.s3.storage.S3Storage` | Tells the connector to use the S3-compatible storage backend (as opposed to HDFS or Azure Blob storage backends available in other Confluent connectors). |

### Output Format

| Parameter | Value | Explanation |
| :--- | :--- | :--- |
| `format.class` | `io.confluent.connect.s3.format.json.JsonFormat` | Controls the file format written to S3. `JsonFormat` writes one JSON object per line (newline-delimited JSON / NDJSON). Alternatives include `io.confluent.connect.s3.format.parquet.ParquetFormat` (columnar, compressed) and `io.confluent.connect.s3.format.avro.AvroFormat`. |

### Flush & Rotation — When Does a File Get Written to S3?

This is one of the most important tuning decisions. Kafka Connect buffers records in memory and writes a file to S3 when **the first condition is met**:

| Parameter | Value | Explanation |
| :--- | :--- | :--- |
| `flush.size` | `15` | **Record count trigger.** Write a file as soon as 15 records have been buffered from a single topic partition. With the simulator generating 15–30 events per batch, a file gets created on roughly every other batch. |
| `rotate.interval.ms` | `60000` | **Time trigger (1 minute).** Even if fewer than 15 records arrived, forcibly flush the buffer to S3 after 60 seconds of inactivity. Prevents events from being stuck in memory indefinitely during low-traffic periods. |

> [!TIP]
> For production workloads, tune `flush.size` higher (e.g. `1000`) and `rotate.interval.ms` longer (e.g. `600000` / 10 minutes) to produce fewer, larger S3 files, which are cheaper to query with analytical engines like DuckDB or Athena.

### Time-Based Partitioning

| Parameter | Value | Explanation |
| :--- | :--- | :--- |
| `partitioner.class` | `io.confluent.connect.storage.partitioner.TimeBasedPartitioner` | Organizes S3 output files into time-based directory hierarchies instead of flat storage. Alternative: `DefaultPartitioner` (uses Kafka partition number only) or `FieldPartitioner` (partitions by a field value inside the message). |
| `path.format` | `'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH` | Template string defining the directory hierarchy. Single-quoted strings (`'year'`) are literal text; unquoted tokens (`YYYY`, `MM`) are Java `DateTimeFormatter` patterns. Produces Hive-compatible key-value paths: `year=2026/month=08/day=24/hour=01/`. |
| `partition.duration.ms` | `3600000` | Duration in milliseconds that each time partition "window" covers. `3600000` = 1 hour. A new subdirectory is created for each hourly window. |
| `timestamp.extractor` | `Wallclock` | Defines which timestamp to use for partitioning. `Wallclock` uses the **wall clock time when Kafka Connect processes the record** (ingestion time). Alternative: `RecordField` extracts a timestamp from inside the JSON payload (e.g. the `timestamp` field in the event envelope). |
| `locale` | `en-US` | Java locale used for date formatting in the `path.format` template. |
| `timezone` | `UTC` | Timezone applied to all timestamps for partition path computation. Using UTC prevents ambiguous partition boundaries during daylight saving time transitions. |

### Schema Compatibility

| Parameter | Value | Explanation |
| :--- | :--- | :--- |
| `schema.compatibility` | `NONE` | How the connector handles schema changes when using Avro or Schema Registry. `NONE` disables schema evolution checks entirely — appropriate here since we use raw JSON without a Schema Registry. |

### Per-Connector Converters

These override the worker-level converter settings from `.docker/kafka-connect/.env` for this specific connector:

| Parameter | Value | Explanation |
| :--- | :--- | :--- |
| `key.converter` | `org.apache.kafka.connect.storage.StringConverter` | Deserializes the Kafka message key as a plain string (`object_id`). |
| `value.converter` | `org.apache.kafka.connect.json.JsonConverter` | Deserializes the Kafka message value as a JSON object for writing to S3. |
| `value.converter.schemas.enable` | `false` | Disables Confluent Schema Registry envelope wrapping. Reads raw JSON objects directly. |

### Error Handling & Dead Letter Queue

| Parameter | Value | Explanation |
| :--- | :--- | :--- |
| `errors.tolerance` | `all` | How the connector reacts to processing failures. `all` means silently skip malformed or unprocessable records and continue. `none` (strict mode) would halt the connector task on the first error. |
| `errors.log.enable` | `true` | Log details of every skipped error record to the Kafka Connect worker log output. |
| `errors.log.include.messages` | `true` | Include the raw record key and value in the error log entry for debugging. |

> [!NOTE]
> In production, you would also configure `errors.deadletterqueue.topic.name=crm-dlq` to route failed records to a dedicated Kafka DLQ topic instead of simply logging and discarding them — as noted in the [TODOS.md](file:///C:/Users/alias/repos/python-kafka-s3-event-pipeline/TODOS.md) roadmap.
