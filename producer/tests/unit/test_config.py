from app.core.config import Settings


def test_settings_defaults():
    settings = Settings(
        KAFKA_BOOTSTRAP_SERVERS="kafka-test:9092",
    )
    assert settings.PROJECT_NAME == "CRM Webhook Producer API"
    assert settings.KAFKA_BOOTSTRAP_SERVERS == "kafka-test:9092"
    assert settings.API_V1_STR == "/api/v1"
    assert settings.KAFKA_CLIENT_ID == "crm-producer-api"
    assert settings.KAFKA_ACKS == "all"
