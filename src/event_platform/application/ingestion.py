"""Ingestion use case: accept a validated event and hand it off for async processing."""

import logging

from event_platform.application.ports import EventPublisher
from event_platform.domain.events import Event

logger = logging.getLogger(__name__)


class EventIngestionService:
    def __init__(self, publisher: EventPublisher) -> None:
        self._publisher = publisher

    async def ingest(self, event: Event) -> None:
        await self._publisher.publish(event)
        logger.debug("Event %s (%s) enqueued", event.event_id, event.event_type)
