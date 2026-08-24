from fastapi import APIRouter, Depends, Query
from app.schemas.responses import SimulationActionResponse, SimulationStatusResponse
from app.services.generator import EventGeneratorService, get_event_generator

router = APIRouter(prefix="/simulation", tags=["Simulation"])


@router.post(
    "/start",
    response_model=SimulationActionResponse,
    summary="Start background CRM event simulation",
    description="Starts generating continuous random CRM events at configurable intervals.",
)
async def start_simulation(
    interval_seconds: int = Query(default=3, ge=1, le=60, description="Interval in seconds between event batches"),
    generator: EventGeneratorService = Depends(get_event_generator),
):
    started = generator.start(interval_seconds=interval_seconds)
    if not started:
        return SimulationActionResponse(status="already_running")
    return SimulationActionResponse(status="started", interval_seconds=interval_seconds)


@router.post(
    "/stop",
    response_model=SimulationActionResponse,
    summary="Stop background CRM event simulation",
    description="Stops the background event generator loop.",
)
async def stop_simulation(
    generator: EventGeneratorService = Depends(get_event_generator),
):
    generator.stop()
    return SimulationActionResponse(status="stopped")


@router.get(
    "/status",
    response_model=SimulationStatusResponse,
    summary="Get simulation status",
    description="Returns whether the event generator background task is currently active.",
)
async def get_simulation_status(
    generator: EventGeneratorService = Depends(get_event_generator),
):
    return SimulationStatusResponse(is_running=generator.is_running)
