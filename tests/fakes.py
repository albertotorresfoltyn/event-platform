"""In-memory test doubles for application ports."""

import asyncio

from event_platform.application.errors import DependencyUnavailableError
from event_platform.application.ports import QueueMessage
from event_platform.application.queries import (
    Cursor,
    EventCount,
    EventFilter,
    RealtimeStats,
    SearchResult,
    TimeBucket,
)
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


class FakeEventIndexer:
    """Dict-backed search index that can be told to fail its next N index calls."""

    def __init__(self, failures: int = 0) -> None:
        self.documents: dict[str, Event] = {}
        self.index_calls = 0
        self.failures_left = failures

    async def index(self, event: Event) -> None:
        self.index_calls += 1
        if self.failures_left > 0:
            self.failures_left -= 1
            raise ConnectionError("search unavailable")
        self.documents[event.event_id] = event


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
        self,
        events: list[Event] | None = None,
        counts: list[EventCount] | None = None,
        counts_by_type: dict[str, int] | None = None,
        delay_seconds: float = 0.0,
    ) -> None:
        self.events = events or []
        self.counts = counts or []
        self.counts_by_type = counts_by_type or {}
        self.delay_seconds = delay_seconds
        self.find_calls: list[tuple[EventFilter, int, Cursor | None]] = []
        self.count_calls: list[tuple[EventFilter, TimeBucket]] = []
        self.count_by_type_calls: list[EventFilter] = []

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

    async def count_by_type(self, event_filter: EventFilter) -> dict[str, int]:
        self.count_by_type_calls.append(event_filter)
        await asyncio.sleep(self.delay_seconds)
        return self.counts_by_type


class StubEventSearcher:
    def __init__(self, result: SearchResult | None = None) -> None:
        self.result = result or SearchResult(total=0, hits=[])
        self.calls: list[tuple[str, EventFilter, int]] = []

    async def search(self, text: str, event_filter: EventFilter, limit: int) -> SearchResult:
        self.calls.append((text, event_filter, limit))
        return self.result


class FakeRealtimeStatsCache:
    def __init__(self, *, available: bool = True) -> None:
        self.stored: RealtimeStats | None = None
        self.ttl_seconds: int | None = None
        self.available = available

    async def get(self) -> RealtimeStats | None:
        self._check()
        return self.stored

    async def set(self, stats: RealtimeStats, ttl_seconds: int) -> None:
        self._check()
        self.stored, self.ttl_seconds = stats, ttl_seconds

    def _check(self) -> None:
        if not self.available:
            raise DependencyUnavailableError("Redis")
