import asyncio
import logging
from random import randint
from typing import Optional
from app.core.kafka import kafka_producer
from app.services.mock_factory import generate_crm_event

logger = logging.getLogger("event_generator")


class EventGeneratorService:
    def __init__(self):
        self.is_running: bool = False
        self._task: Optional[asyncio.Task] = None

    async def _run_loop(self, interval_seconds: int = 3):
        logger.info("Background event simulator started.")
        while self.is_running:
            batch_size = randint(15, 30)
            logger.info(f"Generating batch of {batch_size} random CRM events...")

            for _ in range(batch_size):
                topic, key, payload = generate_crm_event()
                kafka_producer.produce_event(topic, key, payload)

            kafka_producer.flush()
            await asyncio.sleep(interval_seconds)

    def start(self, interval_seconds: int = 3) -> bool:
        """Starts the simulator loop if not already running. Returns True if started, False if already running."""
        if not self.is_running:
            self.is_running = True
            self._task = asyncio.create_task(self._run_loop(interval_seconds))
            return True
        return False

    def stop(self) -> bool:
        """Stops the simulator loop if running. Returns True if stopped, False if was not running."""
        if self.is_running:
            self.is_running = False
            if self._task:
                self._task.cancel()
                self._task = None
            logger.info("Background event simulator stopped.")
            return True
        return False


event_generator = EventGeneratorService()


def get_event_generator() -> EventGeneratorService:
    """Dependency injector for simulator service."""
    return event_generator
