import uuid
from datetime import datetime, timezone
from random import choice, randint
from faker import Faker

fake = Faker()

OBJECT_TYPES = ["contacts", "leads", "deals", "engagements"]

EVENT_TYPES = {
    "contacts": ["contact.created", "contact.updated", "contact.lifecycle_changed"],
    "leads": ["lead.created", "lead.qualified", "lead.status_updated"],
    "deals": ["deal.created", "deal.stage_updated", "deal.closed_won", "deal.closed_lost"],
    "engagements": ["email.sent", "call.completed", "meeting.scheduled"]
}

STAGES = ["qualifying", "value_proposition", "proposal_sent", "contract_sent", "closed_won"]

def generate_crm_event(object_type: str = None) -> tuple[str, str, dict]:
    """Generates a random CRM event payload and returns (topic, key, payload_dict)."""
    if not object_type or object_type not in OBJECT_TYPES:
        object_type = choice(OBJECT_TYPES)

    topic = f"crm-{object_type}"
    object_id = f"{object_type[:3]}_{fake.hexify(text='^^^^^^^^')}"
    event_type = choice(EVENT_TYPES[object_type])

    data = {}
    if object_type == "contacts":
        data = {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.company_email(),
            "lifecycle_stage": choice(["subscriber", "lead", "opportunity", "customer"])
        }
    elif object_type == "leads":
        data = {
            "lead_score": randint(1, 100),
            "source": choice(["organic_search", "paid_ads", "referral", "webinar"]),
            "status": choice(["new", "contacted", "working", "unqualified"])
        }
    elif object_type == "deals":
        data = {
            "deal_name": f"{fake.company()} - {choice(['Software', 'Services', 'Expansion'])}",
            "amount": float(randint(1000, 150000)),
            "currency": "USD",
            "stage": choice(STAGES)
        }
    elif object_type == "engagements":
        data = {
            "channel": choice(["email", "phone_call", "video_call"]),
            "rep_email": fake.email(),
            "duration_seconds": randint(30, 3600) if event_type != "email.sent" else 0,
            "notes": fake.sentence()
        }

    envelope = {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "event_type": event_type,
        "object_type": object_type,
        "object_id": object_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": data
    }

    return topic, object_id, envelope