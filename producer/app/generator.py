import asyncio
import logging
from random import randint
from app.kafka_client import kafka_producer
from app.mock_data import generate_crm_event

logger = logging.getLogger("event_generator")

class EventGenerator:
    def __init__(self):
        self.is_running = False
        self._task = None

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

    def start(self, interval_seconds: int = 3):
        if not self.is_running:
            self.is_running = True
            self._task = asyncio.create_task(self._run_loop(interval_seconds))

    def stop(self):
        if self.is_running:
            self.is_running = False
            if self._task:
                self._task.cancel()
            logger.info("Background event simulator stopped.")

event_generator = EventGenerator()