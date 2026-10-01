"""Pure builders translating read-side models into MongoDB query documents."""

from typing import Any

from event_platform.application.queries import Cursor, EventFilter, TimeBucket

NEWEST_FIRST = [("timestamp", -1), ("_id", -1)]


def build_match(event_filter: EventFilter) -> dict[str, Any]:
    match: dict[str, Any] = {
        field: value
        for field, value in (
            ("event_type", event_filter.event_type),
            ("user_id", event_filter.user_id),
            ("source_url", event_filter.source_url),
        )
        if value is not None
    }
    time_range = {
        operator: bound
        for operator, bound in (("$gte", event_filter.start), ("$lt", event_filter.end))
        if bound is not None
    }
    if time_range:
        match["timestamp"] = time_range
    return match


def build_after_cursor(cursor: Cursor) -> dict[str, Any]:
    """Keyset condition: strictly older than the cursor in (timestamp, _id) order."""
    return {
        "$or": [
            {"timestamp": {"$lt": cursor.timestamp}},
            {"timestamp": cursor.timestamp, "_id": {"$lt": cursor.event_id}},
        ]
    }


def build_find_query(event_filter: EventFilter, after: Cursor | None) -> dict[str, Any]:
    match = build_match(event_filter)
    if after is None:
        return match
    return {"$and": [match, build_after_cursor(after)]} if match else build_after_cursor(after)


def build_count_by_bucket_pipeline(
    event_filter: EventFilter, bucket: TimeBucket
) -> list[dict[str, Any]]:
    return [
        {"$match": build_match(event_filter)},
        {
            "$group": {
                "_id": {
                    "bucket_start": {
                        "$dateTrunc": {
                            "date": "$timestamp",
                            "unit": bucket.value,
                            "timezone": "UTC",
                            "startOfWeek": "monday",
                        }
                    },
                    "event_type": "$event_type",
                },
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"_id.bucket_start": 1, "_id.event_type": 1}},
    ]


def build_count_by_type_pipeline(event_filter: EventFilter) -> list[dict[str, Any]]:
    return [
        {"$match": build_match(event_filter)},
        {"$group": {"_id": "$event_type", "count": {"$sum": 1}}},
    ]
