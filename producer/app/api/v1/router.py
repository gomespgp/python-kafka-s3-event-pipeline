from fastapi import APIRouter
from app.api.v1.endpoints import simulation, webhooks

api_router = APIRouter()
api_router.include_router(webhooks.router)
api_router.include_router(simulation.router)
