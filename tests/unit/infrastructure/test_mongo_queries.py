from datetime import UTC, datetime

from event_platform.application.queries import Cursor, EventFilter, TimeBucket
from event_platform.infrastructure.mongo.queries import (
    build_count_by_bucket_pipeline,
    build_find_query,
    build_match,
)

START = datetime(2026, 10, 1, tzinfo=UTC)
END = datetime(2026, 10, 2, tzinfo=UTC)
CURSOR = Cursor(datetime(2026, 10, 1, 12, tzinfo=UTC), "evt-9")
AFTER_CURSOR = {
    "$or": [
        {"timestamp": {"$lt": CURSOR.timestamp}},
        {"timestamp": CURSOR.timestamp, "_id": {"$lt": "evt-9"}},
    ]
}


def test_empty_filter_matches_everything() -> None:
    assert build_match(EventFilter()) == {}


def test_match_includes_only_provided_fields() -> None:
    event_filter = EventFilter(event_type="click", user_id="u-1", start=START, end=END)

    assert build_match(event_filter) == {
        "event_type": "click",
        "user_id": "u-1",
        "timestamp": {"$gte": START, "$lt": END},
    }


def test_open_ended_range_uses_single_bound() -> None:
    assert build_match(EventFilter(source_url="https://a.io", end=END)) == {
        "source_url": "https://a.io",
        "timestamp": {"$lt": END},
    }


def test_find_query_without_cursor_is_the_match() -> None:
    assert build_find_query(EventFilter(user_id="u-1"), after=None) == {"user_id": "u-1"}


def test_find_query_with_cursor_only() -> None:
    assert build_find_query(EventFilter(), after=CURSOR) == AFTER_CURSOR


def test_find_query_combines_filter_and_cursor() -> None:
    assert build_find_query(EventFilter(user_id="u-1"), after=CURSOR) == {
        "$and": [{"user_id": "u-1"}, AFTER_CURSOR]
    }


def test_stats_pipeline_truncates_timestamps_to_the_bucket() -> None:
    pipeline = build_count_by_bucket_pipeline(EventFilter(start=START, end=END), TimeBucket.WEEK)

    assert pipeline[0] == {"$match": {"timestamp": {"$gte": START, "$lt": END}}}
    date_trunc = pipeline[1]["$group"]["_id"]["bucket_start"]["$dateTrunc"]
    assert date_trunc["unit"] == "week"
    assert date_trunc["startOfWeek"] == "monday"
