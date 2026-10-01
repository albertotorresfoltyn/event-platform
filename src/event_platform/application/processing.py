"""Processing use case: persist one queued event, acking or scheduling a retry."""

import logging

from event_platform.application.backoff import ExponentialBackoff
from event_platform.application.ports import EventConsumer, EventRepository, QueueMessage

logger = logging.getLogger(__name__)


class EventProcessor:
    def __init__(
        self,
        repository: EventRepository,
        consumer: EventConsumer,
        backoff: ExponentialBackoff,
    ) -> None:
        self._repository = repository
        self._consumer = consumer
        self._backoff = backoff

    async def process(self, message: QueueMessage) -> None:
        """Never raises: every outcome ends in an ack or a scheduled retry."""
        event = message.event
        try:
            inserted = await self._repository.save(event)
        except Exception:
            delay = self._backoff.delay_for(message.receive_count)
            logger.warning(
                "Failed to persist event %s (attempt %d), retrying in %.2fs",
                event.event_id,
                message.receive_count,
                delay,
                exc_info=True,
            )
            await self._consumer.retry_later(message.receipt_handle, delay)
            return

        if not inserted:
            logger.info("Skipping duplicate event %s", event.event_id)
        await self._consumer.ack(message.receipt_handle)
