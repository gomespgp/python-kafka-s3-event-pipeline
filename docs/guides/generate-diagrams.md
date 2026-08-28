# 📊 Generating Architecture Diagrams Guide

This guide explains how to generate the visual system architecture diagrams for the Python Kafka S3 Event Pipeline using the [`diagrams`](https://pypi.org/project/diagrams/) Python library ("Diagram as Code").

---

## 1. Overview & Tooling Philosophy

The visual architecture diagram is defined and generated programmatically in Python using the `diagrams` library. 

> [!NOTE]
> The `diagrams` library is only used as a developer documentation utility to render visual diagram assets. It is **not** a runtime service dependency and is **not** included in [producer requirements.txt](file:///C:/Users/alias/repos/python-kafka-s3-event-pipeline/.docker/producer/requirements.txt).

---

## 2. Installation & Prerequisites

Generating the architecture diagram requires two components:
1. The **`diagrams` Python package** (installed in your Python environment).
2. The **Graphviz system binary** (`dot`), which `diagrams` executes behind the scenes to render layout and graphics.

### A. Install `diagrams` (Python Library)

Install `diagrams` into your virtual environment using `uv`:

```bash
uv pip install diagrams
```

*(Or via standard pip: `pip install diagrams`)*

### B. Install Graphviz (System Binary)

Graphviz must be installed and accessible on your system `PATH`:

- **Windows:**
  - Option 1 (Winget): `winget install Graphviz.Graphviz`
  - Option 2 (Manual Installer): Download the official 64-bit EXE installer from [Graphviz Download Page](https://graphviz.org/download/#windows) and make sure to select *"Add Graphviz to the system PATH for all users"* (or for current user) during setup.
  - Option 3 (Chocolatey / Scoop): `choco install graphviz` or `scoop install graphviz`
- **macOS:**
  ```bash
  brew install graphviz
  ```
- **Linux (Debian / Ubuntu):**
  ```bash
  sudo apt-get update && sudo apt-get install -y graphviz
  ```

> [!TIP]
> After installing Graphviz, verify that `dot -V` works in your terminal. If you get `ExecutableNotFound: failed to execute WindowsPath('dot')`, restart your terminal session or add Graphviz `bin` directory (e.g. `C:\Program Files\Graphviz\bin`) to your `PATH` environment variable.

## 3. Diagram Generation Script

The diagram generator script defines the full event streaming topology—including the FastAPI webhook producer, simulated generator, Kafka KRaft broker, Kafka UI, Kafka Connect with S3 sink connector, and MinIO S3 bucket.

Here is the complete Python code:

```python
"""Generate Architecture Diagram for Python Kafka S3 Event Pipeline."""

import os
import shutil
import sys
from pathlib import Path

# Auto-detect Graphviz on Windows if not already in PATH
if not shutil.which("dot"):
    candidate_paths = [
        r"C:\Program Files (x86)\Graphviz\bin",
        r"C:\Program Files\Graphviz\bin",
        os.path.expandvars(r"%LOCALAPPDATA%\Programs\Graphviz\bin"),
    ]
    for p in candidate_paths:
        if Path(p, "dot.exe").is_file():
            os.environ["PATH"] = p + os.pathsep + os.environ["PATH"]
            break

try:
    from diagrams import Cluster, Diagram, Edge
    from diagrams.aws.storage import SimpleStorageServiceS3
    from diagrams.onprem.client import Users
    from diagrams.onprem.container import Docker
    from diagrams.onprem.queue import Kafka
    from diagrams.programming.framework import FastAPI
    from diagrams.programming.language import Python
except ImportError:
    print(
        "Error: 'diagrams' package is not installed.\n"
        "Run: uv pip install diagrams  (or pip install diagrams)",
        file=sys.stderr,
    )
    sys.exit(1)

graph_attr = {
    "fontsize": "18",
    "fontname": "Segoe UI Bold",
    "fontcolor": "#1E293B",
    "bgcolor": "white",
    "pad": "0.6",
    "nodesep": "0.8",
    "ranksep": "1.4",
    "splines": "ortho",
    "dpi": "300",
}

node_attr = {
    "fontname": "Segoe UI Semibold",
    "fontsize": "11",
    "fontcolor": "#0F172A",
    "labelloc": "b",
}

edge_attr = {
    "fontname": "Segoe UI Medium",
    "fontsize": "10",
    "fontcolor": "#334155",
    "color": "#334155",
}

with Diagram(
    name="Python Kafka S3 Event Pipeline Architecture",
    show=False,
    direction="LR",
    filename="docs/architecture/assets/architecture_diagram",
    outformat="png",
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
):
    users = Users("CRM Webhook Clients\n(External)")

    with Cluster("1. Event Ingestion Layer (FastAPI Producer)"):
        fastapi_app = FastAPI("FastAPI REST API\n(:8000)")
        simulator = Python("Simulator Worker\n(Async Faker)")
        producer_client = Python("confluent-kafka\n(Client / DI)")

    with Cluster("2. Streaming Backbone"):
        kafka_broker = Kafka("Kafka Broker\n(KRaft :9092)")
        kafka_ui = Docker("Kafka UI\n(:8088)")

    with Cluster("3. Integration Layer (Kafka Connect)"):
        connect_init = Docker("Init Sidecar\n(Register Sink)")
        kafka_connect = Docker("Kafka Connect\n(:8083)")

    with Cluster("4. Data Lake Storage"):
        minio_s3 = SimpleStorageServiceS3("MinIO S3 Bucket\n(kafka-s3-events-sink)")

    # 1. Ingestion flows (POST Webhooks & Async Batches)
    users >> Edge(label="POST Webhook", color="#2563eb", style="bold", penwidth="1.8", arrowhead="vee") >> fastapi_app
    fastapi_app >> Edge(label="Dispatch", color="#2563eb", penwidth="1.5", arrowhead="vee") >> producer_client
    simulator >> Edge(label="Batches", color="#2563eb", penwidth="1.5", arrowhead="vee") >> producer_client

    # 2. Main Data Pipeline (Produce -> Broker -> Poll -> Sink)
    producer_client >> Edge(label="Produce JSON", color="#2563eb", style="bold", penwidth="2.0", arrowhead="vee") >> kafka_broker
    kafka_broker >> Edge(label="Poll crm-.*", color="#0284c7", style="bold", penwidth="2.0", arrowhead="vee") >> kafka_connect
    kafka_connect >> Edge(label="Time-Partitioned Sink", color="#059669", style="bold", penwidth="2.2", arrowhead="vee") >> minio_s3

    # 3. Control Plane Sidecar (Dashed Slate)
    connect_init >> Edge(label="Register Sink", color="#64748b", style="dashed", penwidth="1.3", arrowhead="vee") >> kafka_connect

    # 4. Observability & Monitoring (Dotted Amber)
    kafka_broker >> Edge(label="Metrics", color="#d97706", style="dotted", penwidth="1.3", arrowhead="vee") >> kafka_ui
```

---

## 4. Generating the Diagram

Execute the script from the root of the repository:

```bash
python scripts/generate_architecture_diagram.py
```

This generates:
- [architecture_diagram.png](file:///C:/Users/alias/repos/python-kafka-s3-event-pipeline/docs/architecture/assets/architecture_diagram.png) under `docs/architecture/assets/`.

---

## 5. Output Asset Location & Usage

The resulting PNG image is referenced directly in:
- [ARCHITECTURE.md](file:///C:/Users/alias/repos/python-kafka-s3-event-pipeline/ARCHITECTURE.md)
- [docs/architecture/overview.md](file:///C:/Users/alias/repos/python-kafka-s3-event-pipeline/docs/architecture/overview.md)
