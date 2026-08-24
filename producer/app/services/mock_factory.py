import uuid
from datetime import datetime, timezone
from random import choice, randint
from typing import Optional
from faker import Faker
from app.schemas.events import CrmObjectType

fake = Faker()

OBJECT_TYPES = [item.value for item in CrmObjectType]

EVENT_TYPES = {
    CrmObjectType.CONTACTS.value: ["contact.created", "contact.updated", "contact.lifecycle_changed"],
    CrmObjectType.LEADS.value: ["lead.created", "lead.qualified", "lead.status_updated"],
    CrmObjectType.DEALS.value: ["deal.created", "deal.stage_updated", "deal.closed_won", "deal.closed_lost"],
    CrmObjectType.ENGAGEMENTS.value: ["email.sent", "call.completed", "meeting.scheduled"],
}

STAGES = ["qualifying", "value_proposition", "proposal_sent", "contract_sent", "closed_won"]


def generate_crm_event(object_type: Optional[str] = None) -> tuple[str, str, dict]:
    """Generates a random CRM event payload and returns (topic, key, payload_dict)."""
    if not object_type or object_type not in OBJECT_TYPES:
        object_type = choice(OBJECT_TYPES)

    topic = f"crm-{object_type}"
    object_id = f"{object_type[:3]}_{fake.hexify(text='^^^^^^^^')}"
    event_type = choice(EVENT_TYPES[object_type])

    data = {}
    if object_type == CrmObjectType.CONTACTS.value:
        data = {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.company_email(),
            "lifecycle_stage": choice(["subscriber", "lead", "opportunity", "customer"]),
        }
    elif object_type == CrmObjectType.LEADS.value:
        data = {
            "lead_score": randint(1, 100),
            "source": choice(["organic_search", "paid_ads", "referral", "webinar"]),
            "status": choice(["new", "contacted", "working", "unqualified"]),
        }
    elif object_type == CrmObjectType.DEALS.value:
        data = {
            "deal_name": f"{fake.company()} - {choice(['Software', 'Services', 'Expansion'])}",
            "amount": float(randint(1000, 150000)),
            "currency": "USD",
            "stage": choice(STAGES),
        }
    elif object_type == CrmObjectType.ENGAGEMENTS.value:
        data = {
            "channel": choice(["email", "phone_call", "video_call"]),
            "rep_email": fake.email(),
            "duration_seconds": randint(30, 3600) if event_type != "email.sent" else 0,
            "notes": fake.sentence(),
        }

    envelope = {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "event_type": event_type,
        "object_type": object_type,
        "object_id": object_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }

    return topic, object_id, envelope
