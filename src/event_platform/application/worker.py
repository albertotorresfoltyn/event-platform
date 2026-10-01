"""Background worker that drains the queue and hands each message to the processor."""

import asyncio
import contextlib
import logging

from event_platform.application.ports import EventConsumer
from event_platform.application.processing import EventProcessor

logger = logging.getLogger(__name__)


class EventWorker:
    def __init__(
        self,
        consumer: EventConsumer,
        processor: EventProcessor,
        *,
        batch_size: int,
        poll_wait_seconds: float,
        error_pause_seconds: float = 1.0,
    ) -> None:
        self._consumer = consumer
        self._processor = processor
        self._batch_size = batch_size
        self._poll_wait_seconds = poll_wait_seconds
        self._error_pause_seconds = error_pause_seconds
        self._stopping = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._task is None:
            self._stopping.clear()
            self._task = asyncio.create_task(self.run(), name="event-worker")
            logger.info("Event worker started")

    async def stop(self, timeout_seconds: float = 10.0) -> None:
        """Finish the in-flight batch, then exit. Unacked messages become visible again."""
        if self._task is None:
            return
        self._stopping.set()
        try:
            await asyncio.wait_for(self._task, timeout=timeout_seconds)
        except TimeoutError:
            logger.warning("Event worker did not stop in %.1fs, cancelling", timeout_seconds)
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None
        logger.info("Event worker stopped")

    async def run(self) -> None:
        while not self._stopping.is_set():
            try:
                await self.run_once()
            except Exception:
                logger.exception("Event worker iteration failed")
                await asyncio.sleep(self._error_pause_seconds)

    async def run_once(self) -> int:
        """Receive and process one batch. Returns the number of messages handled."""
        messages = await self._consumer.receive(self._batch_size, self._poll_wait_seconds)
        await asyncio.gather(*(self._processor.process(message) for message in messages))
        return len(messages)
