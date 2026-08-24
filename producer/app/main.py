from contextlib import asynccontextmanager
from typing import Any, Dict
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel

from app.kafka_client import kafka_producer
from app.mock_data import OBJECT_TYPES
from app.generator import event_generator

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    event_generator.stop()
    kafka_producer.flush()

app = FastAPI(
    title="CRM Webhook Producer API",
    description="Ingests CRM webhooks and streams simulated events to Kafka.",
    version="1.0.0",
    lifespan=lifespan,
    ports={"http": 8000, "https": 8443}
)

class WebhookPayload(BaseModel):
    event_type: str
    object_id: str
    data: Dict[str, Any]

@app.post("/webhooks/crm/{object_type}", status_code=202)
async def receive_webhook(object_type: str, payload: WebhookPayload):
    """Manual endpoint for receiving raw CRM webhooks."""
    if object_type not in OBJECT_TYPES:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid object_type. Must be one of: {OBJECT_TYPES}"
        )

    topic = f"crm-{object_type}"
    event_envelope = {
        "event_type": payload.event_type,
        "object_type": object_type,
        "object_id": payload.object_id,
        "data": payload.data
    }

    kafka_producer.produce_event(
        topic=topic,
        key=payload.object_id,
        payload=event_envelope
    )
    return {"status": "accepted", "topic": topic, "object_id": payload.object_id}

@app.post("/simulation/start")
async def start_simulation(interval_seconds: int = 3):
    """Starts generating continuous random CRM events in the background."""
    if event_generator.is_running:
        return {"status": "already_running"}
    event_generator.start(interval_seconds=interval_seconds)
    return {"status": "started", "interval_seconds": interval_seconds}

@app.post("/simulation/stop")
async def stop_simulation():
    """Stops the background event generator."""
    event_generator.stop()
    return {"status": "stopped"}

@app.get("/simulation/status")
async def simulation_status():
    """Returns generator running status."""
    return {"is_running": event_generator.is_running}