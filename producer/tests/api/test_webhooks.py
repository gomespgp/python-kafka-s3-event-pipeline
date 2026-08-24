def test_ingest_contact_webhook_success(client, mock_kafka_producer):
    payload = {
        "event_type": "contact.created",
        "object_id": "con_112233",
        "data": {
            "first_name": "Bob",
            "last_name": "Marley",
            "email": "bob@example.com",
            "lifecycle_stage": "customer",
        },
    }
    response = client.post("/api/v1/webhooks/crm/contacts", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "accepted"
    assert data["topic"] == "crm-contacts"
    assert data["object_id"] == "con_112233"

    mock_kafka_producer.produce_event.assert_called_once()
    args, kwargs = mock_kafka_producer.produce_event.call_args
    assert kwargs["topic"] == "crm-contacts"
    assert kwargs["key"] == "con_112233"
    assert kwargs["payload"]["event_type"] == "contact.created"


def test_ingest_webhook_lead_success(client, mock_kafka_producer):
    payload = {
        "event_type": "lead.created",
        "object_id": "lea_99999",
        "data": {"lead_score": 90, "status": "new"},
    }
    response = client.post("/api/v1/webhooks/crm/leads", json=payload)
    assert response.status_code == 202
    assert response.json()["topic"] == "crm-leads"


def test_ingest_webhook_invalid_object_type(client, mock_kafka_producer):
    payload = {
        "event_type": "account.created",
        "object_id": "acc_111",
        "data": {},
    }
    response = client.post("/api/v1/webhooks/crm/accounts", json=payload)
    assert response.status_code == 422
    mock_kafka_producer.produce_event.assert_not_called()


def test_ingest_webhook_missing_required_fields(client, mock_kafka_producer):
    payload = {"event_type": "contact.created"}  # missing object_id and data
    response = client.post("/api/v1/webhooks/crm/contacts", json=payload)
    assert response.status_code == 422
    mock_kafka_producer.produce_event.assert_not_called()
