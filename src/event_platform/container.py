"""Dependency container: the one place where concrete adapters are chosen."""

from dataclasses import dataclass

from event_platform.application.ingestion import EventIngestionService
from event_platform.config import Settings
from event_platform.infrastructure.queue.in_memory import InMemoryEventQueue


@dataclass(frozen=True, slots=True)
class Container:
    queue: InMemoryEventQueue
    ingestion_service: EventIngestionService


def build_container(settings: Settings) -> Container:
    queue = InMemoryEventQueue(
        max_size=settings.queue_max_size,
        visibility_timeout_seconds=settings.queue_visibility_timeout_seconds,
        max_receive_count=settings.queue_max_receive_count,
    )
    return Container(queue=queue, ingestion_service=EventIngestionService(queue))
