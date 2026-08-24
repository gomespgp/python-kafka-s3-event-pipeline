from app.schemas.events import CrmObjectType
from app.services.mock_factory import OBJECT_TYPES, generate_crm_event


def test_object_types_list():
    assert set(OBJECT_TYPES) == {"contacts", "leads", "deals", "engagements"}


def test_generate_crm_event_random():
    topic, object_id, envelope = generate_crm_event()
    assert topic.startswith("crm-")
    assert envelope["object_type"] in OBJECT_TYPES
    assert envelope["event_id"].startswith("evt_")
    assert "timestamp" in envelope
    assert isinstance(envelope["data"], dict)


def test_generate_crm_event_specific_types():
    for obj_type in [CrmObjectType.CONTACTS.value, CrmObjectType.LEADS.value, CrmObjectType.DEALS.value, CrmObjectType.ENGAGEMENTS.value]:
        topic, object_id, envelope = generate_crm_event(object_type=obj_type)
        assert topic == f"crm-{obj_type}"
        assert envelope["object_type"] == obj_type
        assert envelope["object_id"] == object_id
        assert len(envelope["data"]) > 0
