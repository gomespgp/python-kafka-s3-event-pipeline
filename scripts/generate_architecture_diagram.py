"""Generate Architecture Diagram for Python Kafka S3 Event Pipeline.

This script uses the 'diagrams' package (Diagram-as-Code) to generate the visual
architecture diagram for documentation.

Prerequisites:
    uv pip install diagrams
"""

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
    "splines": "lines",
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
        with Cluster("Sources"):
            fastapi_app = FastAPI("FastAPI API\n(:8000)")
            simulator = Python("Simulator Worker\n(Async Faker)")
        producer_client = Python("confluent-kafka Client\n(core/kafka.py)")

    with Cluster("2. Streaming & Connector Backbone"):
        with Cluster("Kafka Cluster"):
            kafka_broker = Kafka("Apache Kafka\n(KRaft Mode :9092)")
            kafka_ui = Docker("Kafka UI\n(:8088)")
        with Cluster("Connector Runtime"):
            kafka_connect = Docker("Kafka Connect S3 Sink\n(:8083)")
            connect_init = Docker("Init Container\n(Auto-register)")

    with Cluster("3. Data Lake Storage"):
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
