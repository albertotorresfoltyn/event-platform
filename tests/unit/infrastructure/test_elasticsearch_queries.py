from datetime import UTC, datetime

from event_platform.application.queries import EventFilter
from event_platform.infrastructure.elasticsearch.queries import build_filters, build_search_query

START = datetime(2026, 10, 1, tzinfo=UTC)


def test_search_query_scores_text_and_filters_structured_fields() -> None:
    query = build_search_query("firefox mobile", EventFilter(event_type="click", start=START))

    [text_query] = query["bool"]["must"]
    assert text_query["simple_query_string"]["query"] == "firefox mobile"
    assert text_query["simple_query_string"]["default_operator"] == "and"
    assert query["bool"]["filter"] == [
        {"term": {"event_type": "click"}},
        {"range": {"timestamp": {"gte": "2026-10-01T00:00:00+00:00"}}},
    ]


def test_no_filters_when_filter_is_empty() -> None:
    assert build_filters(EventFilter()) == []


def test_every_exact_field_becomes_a_term_filter() -> None:
    filters = build_filters(EventFilter(event_type="a", user_id="u", source_url="https://x.io"))

    assert filters == [
        {"term": {"event_type": "a"}},
        {"term": {"user_id": "u"}},
        {"term": {"source_url": "https://x.io"}},
    ]
