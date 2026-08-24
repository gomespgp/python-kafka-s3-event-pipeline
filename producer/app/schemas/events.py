from enum import Enum
from typing import Any
from pydantic import BaseModel, Field


class CrmObjectType(str, Enum):
    CONTACTS = "contacts"
    LEADS = "leads"
    DEALS = "deals"
    ENGAGEMENTS = "engagements"


class WebhookPayload(BaseModel):
    event_type: str = Field(..., description="Action name e.g. contact.created, deal.won", example="contact.created")
    object_id: str = Field(..., description="Unique entity ID in source CRM", example="con_998877")
    data: dict[str, Any] = Field(..., description="Payload attributes of the CRM entity", example={"first_name": "Alice", "email": "alice@example.com"})


class EventEnvelope(BaseModel):
    event_id: str = Field(..., description="Unique UUID for this event occurrence")
    event_type: str = Field(..., description="Specific event action")
    object_type: str = Field(..., description="CRM object type category")
    object_id: str = Field(..., description="Unique entity ID")
    timestamp: str = Field(..., description="ISO 8601 UTC timestamp")
    data: dict[str, Any] = Field(..., description="Entity payload data")
