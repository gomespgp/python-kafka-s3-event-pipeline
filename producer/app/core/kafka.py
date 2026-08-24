import json
import logging
from typing import Any, Optional
from confluent_kafka import Producer
from app.core.config import settings

logger = logging.getLogger("kafka_producer")
logging.basicConfig(level=logging.INFO)


class KafkaEventProducer:
    def __init__(self):
        config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": settings.KAFKA_CLIENT_ID,
            "acks": settings.KAFKA_ACKS,
            "retries": settings.KAFKA_RETRIES,
        }
        self.producer = Producer(config)
        logger.info(f"Kafka Producer initialized pointing to {settings.KAFKA_BOOTSTRAP_SERVERS}")

    def _delivery_callback(self, err, msg):
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(
                f"Delivered event to topic '{msg.topic()}' [Partition {msg.partition()}] "
                f"with key '{msg.key().decode('utf-8') if msg.key() else 'None'}'"
            )

    def produce_event(self, topic: str, key: Optional[str], payload: dict[str, Any]) -> None:
        """Serializes and sends a record to Kafka asynchronously."""
        serialized_value = json.dumps(payload).encode("utf-8")
        serialized_key = key.encode("utf-8") if key else None

        self.producer.produce(
            topic=topic,
            key=serialized_key,
            value=serialized_value,
            callback=self._delivery_callback,
        )
        # Serve delivery callbacks from previous requests
        self.producer.poll(0)

    def flush(self, timeout: float = 5.0) -> int:
        """Forces buffered messages to be delivered to Kafka."""
        return self.producer.flush(timeout)


kafka_producer = KafkaEventProducer()


def get_kafka_producer() -> KafkaEventProducer:
    """Dependency injector for FastAPI routes."""
    return kafka_producer
