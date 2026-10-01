"""Query use cases: paginated event listing and time-bucketed statistics."""

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime

from event_platform.application.errors import InvalidQueryError
from event_platform.application.ports import EventReader
from event_platform.application.queries import (
    Cursor,
    EventFilter,
    EventPage,
    EventStats,
    TimeBucket,
)

MAX_STATS_BUCKETS = 1_000
DEFAULT_STATS_BUCKETS = {TimeBucket.HOUR: 24, TimeBucket.DAY: 30, TimeBucket.WEEK: 12}


def _utc_now() -> datetime:
    return datetime.now(UTC)


class EventQueryService:
    def __init__(self, reader: EventReader, clock: Callable[[], datetime] = _utc_now) -> None:
        self._reader = reader
        self._clock = clock

    async def list_events(
        self, event_filter: EventFilter, limit: int, cursor: Cursor | None = None
    ) -> EventPage:
        # Fetch one extra row to know whether another page exists without a count query.
        events = await self._reader.find(event_filter, limit + 1, cursor)
        page, has_more = events[:limit], len(events) > limit
        next_cursor = Cursor(page[-1].timestamp, page[-1].event_id) if has_more else None
        return EventPage(items=page, next_cursor=next_cursor)

    async def stats(self, event_filter: EventFilter, bucket: TimeBucket) -> EventStats:
        start, end = self._stats_window(event_filter, bucket)
        counts = await self._reader.count_by_bucket(
            replace(event_filter, start=start, end=end), bucket
        )
        return EventStats(bucket=bucket, start=start, end=end, counts=counts)

    def _stats_window(
        self, event_filter: EventFilter, bucket: TimeBucket
    ) -> tuple[datetime, datetime]:
        """Default missing bounds and cap the window so one request cannot scan everything."""
        end = event_filter.end or self._clock()
        start = event_filter.start or end - bucket.duration * DEFAULT_STATS_BUCKETS[bucket]
        if start >= end:
            raise InvalidQueryError("start must be earlier than end")
        if (end - start) / bucket.duration > MAX_STATS_BUCKETS:
            raise InvalidQueryError(
                f"time range spans more than {MAX_STATS_BUCKETS} {bucket.value} buckets"
            )
        return start, end
