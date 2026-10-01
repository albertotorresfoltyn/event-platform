"""Pure builders for Elasticsearch query DSL."""

from typing import Any

from event_platform.application.queries import EventFilter

SEARCH_FIELDS = ["metadata_text", "source_url.text^0.5"]
BEST_MATCH_THEN_NEWEST: list[Any] = ["_score", {"timestamp": "desc"}, {"event_id": "desc"}]


def build_search_query(text: str, event_filter: EventFilter) -> dict[str, Any]:
    """Full-text match scores the hits; structured filters run in (cached) filter context.

    ``simple_query_string`` is used instead of ``query_string`` because it never
    raises on malformed user syntax, which matters for an endpoint fed by end users.
    """
    return {
        "bool": {
            "must": [
                {
                    "simple_query_string": {
                        "query": text,
                        "fields": SEARCH_FIELDS,
                        "default_operator": "and",
                    }
                }
            ],
            "filter": build_filters(event_filter),
        }
    }


def build_filters(event_filter: EventFilter) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = [
        {"term": {field: value}}
        for field, value in (
            ("event_type", event_filter.event_type),
            ("user_id", event_filter.user_id),
            ("source_url", event_filter.source_url),
        )
        if value is not None
    ]
    time_range = {
        operator: bound.isoformat()
        for operator, bound in (("gte", event_filter.start), ("lt", event_filter.end))
        if bound is not None
    }
    if time_range:
        filters.append({"range": {"timestamp": time_range}})
    return filters
