from fastapi import APIRouter, Depends, status
from app.core.kafka import KafkaEventProducer, get_kafka_producer
from app.schemas.events import CrmObjectType, WebhookPayload
from app.schemas.responses import WebhookAcceptedResponse

router = APIRouter(prefix="/webhooks", tags=["Webhooks"])


@router.post(
    "/crm/{object_type}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=WebhookAcceptedResponse,
    summary="Ingest manual CRM webhook",
    description="Receives raw CRM webhook payloads and forwards them to the matching Kafka topic.",
)
async def receive_webhook(
    object_type: CrmObjectType,
    payload: WebhookPayload,
    producer: KafkaEventProducer = Depends(get_kafka_producer),
):
    topic = f"crm-{object_type.value}"
    event_envelope = {
        "event_type": payload.event_type,
        "object_type": object_type.value,
        "object_id": payload.object_id,
        "data": payload.data,
    }

    producer.produce_event(
        topic=topic,
        key=payload.object_id,
        payload=event_envelope,
    )
    return WebhookAcceptedResponse(
        status="accepted",
        topic=topic,
        object_id=payload.object_id,
    )
