from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PROJECT_NAME: str = "CRM Webhook Producer API"
    PROJECT_DESCRIPTION: str = "Ingests CRM webhooks and streams simulated events to Kafka."
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"
    
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CLIENT_ID: str = "crm-producer-api"
    KAFKA_ACKS: str = "all"
    KAFKA_RETRIES: int = 5

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
