"""In-memory test doubles for application ports."""

from event_platform.application.ports import QueueMessage
from event_platform.application.queries import Cursor, EventCount, EventFilter, TimeBucket
from event_platform.domain.events import Event


class FakeEventRepository:
    """Dict-backed repository that can be told to fail its next N saves."""

    def __init__(self, failures: int = 0) -> None:
        self.events: dict[str, Event] = {}
        self.failures_left = failures

    async def save(self, event: Event) -> bool:
        if self.failures_left > 0:
            self.failures_left -= 1
            raise ConnectionError("database unavailable")
        if event.event_id in self.events:
            return False
        self.events[event.event_id] = event
        return True


class RecordingConsumer:
    """Consumer that records acks and retries instead of acting on a queue."""

    def __init__(self) -> None:
        self.acked: list[str] = []
        self.retried: list[tuple[str, float]] = []

    async def receive(self, max_messages: int, wait_seconds: float) -> list[QueueMessage]:
        return []

    async def ack(self, receipt_handle: str) -> None:
        self.acked.append(receipt_handle)

    async def retry_later(self, receipt_handle: str, delay_seconds: float) -> None:
        self.retried.append((receipt_handle, delay_seconds))


class StubEventReader:
    """Returns canned results and records the arguments it was called with."""

    def __init__(
        self, events: list[Event] | None = None, counts: list[EventCount] | None = None
    ) -> None:
        self.events = events or []
        self.counts = counts or []
        self.find_calls: list[tuple[EventFilter, int, Cursor | None]] = []
        self.count_calls: list[tuple[EventFilter, TimeBucket]] = []

    async def find(
        self, event_filter: EventFilter, limit: int, after: Cursor | None
    ) -> list[Event]:
        self.find_calls.append((event_filter, limit, after))
        return self.events[:limit]

    async def count_by_bucket(
        self, event_filter: EventFilter, bucket: TimeBucket
    ) -> list[EventCount]:
        self.count_calls.append((event_filter, bucket))
        return self.counts
