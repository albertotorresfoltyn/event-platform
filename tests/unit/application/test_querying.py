from datetime import UTC, datetime, timedelta

import pytest

from event_platform.application.errors import InvalidQueryError
from event_platform.application.queries import Cursor, EventFilter, TimeBucket
from event_platform.application.querying import MAX_STATS_BUCKETS, EventQueryService
from tests.factories import make_event
from tests.fakes import StubEventReader

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def make_service(reader: StubEventReader) -> EventQueryService:
    return EventQueryService(reader, clock=lambda: NOW)


async def test_list_returns_cursor_when_more_events_exist() -> None:
    events = [make_event(timestamp=NOW - timedelta(minutes=i)) for i in range(3)]
    reader = StubEventReader(events=events)

    page = await make_service(reader).list_events(EventFilter(), limit=2)

    assert page.items == events[:2]
    assert page.next_cursor == Cursor(events[1].timestamp, events[1].event_id)
    assert reader.find_calls[0][1] == 3


async def test_list_has_no_cursor_on_last_page() -> None:
    reader = StubEventReader(events=[make_event()])

    page = await make_service(reader).list_events(EventFilter(), limit=2)

    assert page.next_cursor is None


async def test_list_forwards_filter_and_cursor() -> None:
    reader = StubEventReader()
    event_filter = EventFilter(event_type="click")
    cursor = Cursor(NOW, "evt-1")

    await make_service(reader).list_events(event_filter, limit=10, cursor=cursor)

    assert reader.find_calls == [(event_filter, 11, cursor)]


@pytest.mark.parametrize(
    ("bucket", "expected_span"),
    [
        (TimeBucket.HOUR, timedelta(hours=24)),
        (TimeBucket.DAY, timedelta(days=30)),
        (TimeBucket.WEEK, timedelta(weeks=12)),
    ],
)
async def test_stats_defaults_to_a_recent_window(
    bucket: TimeBucket, expected_span: timedelta
) -> None:
    reader = StubEventReader()

    stats = await make_service(reader).stats(EventFilter(event_type="click"), bucket)

    assert (stats.start, stats.end) == (NOW - expected_span, NOW)
    [(forwarded, forwarded_bucket)] = reader.count_calls
    assert forwarded == EventFilter(event_type="click", start=stats.start, end=NOW)
    assert forwarded_bucket is bucket


async def test_stats_rejects_window_with_too_many_buckets() -> None:
    too_wide = EventFilter(start=NOW - timedelta(hours=MAX_STATS_BUCKETS + 1), end=NOW)

    with pytest.raises(InvalidQueryError, match="more than"):
        await make_service(StubEventReader()).stats(too_wide, TimeBucket.HOUR)


async def test_stats_rejects_start_in_the_future_without_end() -> None:
    with pytest.raises(InvalidQueryError, match="start must be earlier"):
        await make_service(StubEventReader()).stats(
            EventFilter(start=NOW + timedelta(days=1)), TimeBucket.DAY
        )
