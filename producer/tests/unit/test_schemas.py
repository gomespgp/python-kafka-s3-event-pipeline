import pytest
from pydantic import ValidationError
from app.schemas.events import CrmObjectType, EventEnvelope, WebhookPayload
from app.schemas.responses import SimulationActionResponse, SimulationStatusResponse, WebhookAcceptedResponse


def test_crm_object_types():
    assert CrmObjectType.CONTACTS.value == "contacts"
    assert CrmObjectType.LEADS.value == "leads"
    assert CrmObjectType.DEALS.value == "deals"
    assert CrmObjectType.ENGAGEMENTS.value == "engagements"


def test_webhook_payload_valid():
    payload = WebhookPayload(
        event_type="contact.created",
        object_id="con_12345",
        data={"first_name": "John", "email": "john@example.com"},
    )
    assert payload.event_type == "contact.created"
    assert payload.object_id == "con_12345"
    assert payload.data["first_name"] == "John"


def test_webhook_payload_missing_field():
    with pytest.raises(ValidationError):
        WebhookPayload(event_type="contact.created")  # missing object_id and data


def test_event_envelope_valid():
    envelope = EventEnvelope(
        event_id="evt_123456",
        event_type="deal.won",
        object_type="deals",
        object_id="dea_9988",
        timestamp="2026-08-24T22:00:00+00:00",
        data={"amount": 5000.0, "currency": "USD"},
    )
    assert envelope.event_id == "evt_123456"
    assert envelope.object_type == "deals"


def test_responses_schemas():
    webhook_res = WebhookAcceptedResponse(topic="crm-leads", object_id="lea_123")
    assert webhook_res.status == "accepted"
    assert webhook_res.topic == "crm-leads"

    sim_res = SimulationStatusResponse(is_running=True)
    assert sim_res.is_running is True

    action_res = SimulationActionResponse(status="started", interval_seconds=5)
    assert action_res.status == "started"
    assert action_res.interval_seconds == 5
