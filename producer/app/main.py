from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.router import api_router
from app.core.config import settings
from app.core.kafka import kafka_producer
from app.services.generator import event_generator


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    event_generator.stop()
    kafka_producer.flush()


app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.PROJECT_DESCRIPTION,
    version=settings.VERSION,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# Include versioned API router under /api/v1 prefix
app.include_router(api_router, prefix=settings.API_V1_STR)


@app.get("/health", tags=["Health"], summary="Healthcheck endpoint")
async def healthcheck():
    return {"status": "ok", "version": settings.VERSION}