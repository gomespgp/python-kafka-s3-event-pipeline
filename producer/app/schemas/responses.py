from typing import Optional
from pydantic import BaseModel


class WebhookAcceptedResponse(BaseModel):
    status: str = "accepted"
    topic: str
    object_id: str


class SimulationStatusResponse(BaseModel):
    is_running: bool


class SimulationActionResponse(BaseModel):
    status: str
    interval_seconds: Optional[int] = None
