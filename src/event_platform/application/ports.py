"""Ports (interfaces) the application layer depends on.

Producer and consumer sides of the queue are separate protocols (interface
segregation): the API only needs to publish, the worker only needs to consume.
"""

from dataclasses import dataclass
from typing import Protocol

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
