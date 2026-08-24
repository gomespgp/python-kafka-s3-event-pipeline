from unittest.mock import MagicMock
import pytest
from fastapi.testclient import TestClient
from app.core.kafka import KafkaEventProducer, get_kafka_producer
from app.main import app
from app.services.generator import EventGeneratorService, get_event_generator


@pytest.fixture
def mock_kafka_producer():
    """Mock for the KafkaEventProducer client."""
    mock = MagicMock(spec=KafkaEventProducer)
    mock.produce_event.return_value = None
    mock.flush.return_value = 0
    return mock


@pytest.fixture
def mock_event_generator():
    """Mock for the EventGeneratorService."""
    mock = MagicMock(spec=EventGeneratorService)
    mock.is_running = False
    mock.start.return_value = True
    mock.stop.return_value = True
    return mock


@pytest.fixture
def client(mock_kafka_producer, mock_event_generator):
    """FastAPI TestClient with overridden Kafka and Simulator dependencies."""
    app.dependency_overrides[get_kafka_producer] = lambda: mock_kafka_producer
    app.dependency_overrides[get_event_generator] = lambda: mock_event_generator

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()
