from datetime import UTC, datetime, timedelta

import pytest
from elasticsearch import AsyncElasticsearch

from event_platform.application.queries import EventFilter
from event_platform.domain.events import Event
from event_platform.infrastructure.elasticsearch.event_index import ElasticsearchEventIndex
from event_platform.infrastructure.elasticsearch.mapping import MAPPINGS
from tests.factories import make_event

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 1, 12, tzinfo=UTC)


async def index_all(
    search_index: ElasticsearchEventIndex,
    client: AsyncElasticsearch,
    index_name: str,
    *events: Event,
) -> None:
    for event in events:
        await search_index.index(event)
    await client.indices.refresh(index=index_name)


async def search_ids(
    search_index: ElasticsearchEventIndex, text: str, event_filter: EventFilter | None = None
) -> list[str]:
    result = await search_index.search(text, event_filter or EventFilter(), 10)
    return [hit.event.event_id for hit in result.hits]


async def test_ensure_index_applies_the_mapping_and_is_idempotent(
    search_index: ElasticsearchEventIndex, elasticsearch_client: AsyncElasticsearch, index_name: str
) -> None:
    await search_index.ensure_index()

    mapping = await elasticsearch_client.indices.get_mapping(index=index_name)
    assert mapping[index_name]["mappings"]["dynamic"] == "strict"
    assert mapping[index_name]["mappings"]["properties"].keys() == MAPPINGS["properties"].keys()


async def test_full_text_matches_nested_metadata_values(
    search_index: ElasticsearchEventIndex, elasticsearch_client: AsyncElasticsearch, index_name: str
) -> None:
    match = make_event(
        metadata={"device": {"os": "iOS", "model": "iPhone"}, "campaign": "Spring Sale"}
    )
    other = make_event(metadata={"device": {"os": "Android"}, "campaign": "Winter"})
    await index_all(search_index, elasticsearch_client, index_name, match, other)

    assert await search_ids(search_index, "ios spring") == [match.event_id]
    assert await search_ids(search_index, "ios winter") == []  # default operator is AND
    assert set(await search_ids(search_index, "ios | winter")) == {match.event_id, other.event_id}


async def test_analysis_folds_case_and_accents(
    search_index: ElasticsearchEventIndex, elasticsearch_client: AsyncElasticsearch, index_name: str
) -> None:
    event = make_event(metadata={"city": "Bogotá"})
    await index_all(search_index, elasticsearch_client, index_name, event)

    assert await search_ids(search_index, "BOGOTA") == [event.event_id]


async def test_url_parts_are_searchable(
    search_index: ElasticsearchEventIndex, elasticsearch_client: AsyncElasticsearch, index_name: str
) -> None:
    event = make_event(source_url="https://shop.example.com/checkout/payment?step=2", metadata={})
    await index_all(search_index, elasticsearch_client, index_name, event)

    assert await search_ids(search_index, "checkout") == [event.event_id]


async def test_structured_filters_narrow_full_text_results(
    search_index: ElasticsearchEventIndex, elasticsearch_client: AsyncElasticsearch, index_name: str
) -> None:
    metadata = {"browser": "firefox"}
    click = make_event(event_type="click", user_id="u-1", timestamp=NOW, metadata=metadata)
    old_click = make_event(
        event_type="click", user_id="u-1", timestamp=NOW - timedelta(days=3), metadata=metadata
    )
    view = make_event(event_type="pageview", user_id="u-1", timestamp=NOW, metadata=metadata)
    await index_all(search_index, elasticsearch_client, index_name, click, old_click, view)

    found = await search_ids(
        search_index,
        "firefox",
        EventFilter(event_type="click", user_id="u-1", start=NOW - timedelta(days=1)),
    )

    assert found == [click.event_id]


async def test_reindexing_the_same_event_does_not_duplicate_it(
    search_index: ElasticsearchEventIndex, elasticsearch_client: AsyncElasticsearch, index_name: str
) -> None:
    event = make_event(metadata={"browser": "opera"})
    await index_all(search_index, elasticsearch_client, index_name, event, event)

    result = await search_index.search("opera", EventFilter(), 10)

    assert result.total == 1
    assert result.hits[0].event == event
    assert result.hits[0].score > 0


async def test_first_write_creates_index_with_explicit_mapping(
    elasticsearch_client: AsyncElasticsearch, index_name: str
) -> None:
    fresh_index = ElasticsearchEventIndex(elasticsearch_client, index_name)

    await fresh_index.index(make_event())

    mapping = await elasticsearch_client.indices.get_mapping(index=index_name)
    assert mapping[index_name]["mappings"]["dynamic"] == "strict"


async def test_search_before_any_event_is_indexed_returns_nothing(
    elasticsearch_client: AsyncElasticsearch, index_name: str
) -> None:
    result = await ElasticsearchEventIndex(elasticsearch_client, index_name).search(
        "anything", EventFilter(), 10
    )

    assert (result.total, result.hits) == (0, [])
