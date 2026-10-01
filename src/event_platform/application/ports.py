"""Ports (interfaces) the application layer depends on.

Producer and consumer sides of the queue are separate protocols (interface
segregation): the API only needs to publish, the worker only needs to consume.
"""

from dataclasses import dataclass
from typing import Protocol

from event_platform.application.queries import (
    Cursor,
    EventCount,
    EventFilter,
    RealtimeStats,
    SearchResult,
    TimeBucket,
)
from event_platform.domain.events import Event


@dataclass(frozen=True, slots=True)
class QueueMessage:
    receipt_handle: str
    event: Event
    receive_count: int


class EventPublisher(Protocol):
    async def publish(self, event: Event) -> None:
        """Enqueue an event. Raises ``QueueFullError`` when at capacity."""


class EventConsumer(Protocol):
    async def receive(self, max_messages: int, wait_seconds: float) -> list[QueueMessage]:
        """Long-poll for up to ``max_messages`` visible messages."""

    async def ack(self, receipt_handle: str) -> None:
        """Delete a successfully processed message."""

    async def retry_later(self, receipt_handle: str, delay_seconds: float) -> None:
        """Make a message visible again after ``delay_seconds`` (backoff)."""


class EventRepository(Protocol):
    async def save(self, event: Event) -> bool:
        """Persist an event idempotently.

        Returns ``False`` when an event with the same ``event_id`` already exists,
        which makes redelivered queue messages safe to process again.
        """


class EventReader(Protocol):
    """Read side of the event store, kept apart from the write path (CQRS-lite)."""

    async def find(
        self, event_filter: EventFilter, limit: int, after: Cursor | None
    ) -> list[Event]:
        """Events matching the filter, newest first, strictly after ``after``."""

    async def count_by_bucket(
        self, event_filter: EventFilter, bucket: TimeBucket
    ) -> list[EventCount]:
        """Event counts grouped by (time bucket, event type), oldest bucket first."""

    async def count_by_type(self, event_filter: EventFilter) -> dict[str, int]:
        """Event counts grouped by event type."""


class EventIndexer(Protocol):
    async def index(self, event: Event) -> None:
        """Make an event searchable. Must be idempotent (keyed by ``event_id``)."""


class EventSearcher(Protocol):
    async def search(self, text: str, event_filter: EventFilter, limit: int) -> SearchResult:
        """Full-text search over event metadata, best match first."""


class RealtimeStatsCache(Protocol):
    """Raises ``DependencyUnavailableError`` when the cache cannot be reached."""

    async def get(self) -> RealtimeStats | None: ...

    async def set(self, stats: RealtimeStats, ttl_seconds: int) -> None: ...
