"""Processing use case: persist and index one queued event, then ack or retry.

MongoDB is the source of truth and is written first; Elasticsearch is a derived
read model. Both writes are idempotent (keyed by ``event_id``), so a message that
failed half-way can simply be redelivered and processed again from the top.
"""

import logging

from event_platform.application.backoff import ExponentialBackoff
from event_platform.application.ports import (
    EventConsumer,
    EventIndexer,
    EventRepository,
    QueueMessage,
)
from event_platform.domain.events import Event

logger = logging.getLogger(__name__)


class EventProcessor:
    def __init__(
        self,
        repository: EventRepository,
        indexer: EventIndexer,
        consumer: EventConsumer,
        backoff: ExponentialBackoff,
    ) -> None:
        self._repository = repository
        self._indexer = indexer
        self._consumer = consumer
        self._backoff = backoff

    async def process(self, message: QueueMessage) -> None:
        """Never raises: every outcome ends in an ack or a scheduled retry."""
        try:
            await self._store(message.event)
        except Exception:
            delay = self._backoff.delay_for(message.receive_count)
            logger.warning(
                "Failed to process event %s (attempt %d), retrying in %.2fs",
                message.event.event_id,
                message.receive_count,
                delay,
                exc_info=True,
            )
            await self._consumer.retry_later(message.receipt_handle, delay)
            return
        await self._consumer.ack(message.receipt_handle)

    async def _store(self, event: Event) -> None:
        if not await self._repository.save(event):
            logger.info("Event %s already stored, re-indexing only", event.event_id)
        await self._indexer.index(event)
