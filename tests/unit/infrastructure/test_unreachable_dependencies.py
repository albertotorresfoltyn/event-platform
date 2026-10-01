"""Adapters must surface an outage as DependencyUnavailableError, not a driver error.

Pointed at a closed local port, so no running services are needed.
"""

from collections.abc import AsyncIterator

import pytest
from elasticsearch import AsyncElasticsearch
from pymongo import AsyncMongoClient

from event_platform.application.errors import DependencyUnavailableError
from event_platform.application.queries import EventFilter, TimeBucket
from event_platform.infrastructure.elasticsearch.event_index import ElasticsearchEventIndex
from event_platform.infrastructure.mongo.documents import Document
from event_platform.infrastructure.mongo.event_reader import MongoEventReader
from tests.factories import make_event

CLOSED_PORT_URL = "localhost:1"


@pytest.fixture
async def mongo_reader() -> AsyncIterator[MongoEventReader]:
    client: AsyncMongoClient[Document] = AsyncMongoClient(
        f"mongodb://{CLOSED_PORT_URL}", serverSelectionTimeoutMS=50
    )
    yield MongoEventReader(client["db"]["events"])
    await client.close()


@pytest.fixture
async def search_index() -> AsyncIterator[ElasticsearchEventIndex]:
    client = AsyncElasticsearch(f"http://{CLOSED_PORT_URL}", request_timeout=0.5, max_retries=0)
    yield ElasticsearchEventIndex(client, "events")
    await client.close()


async def test_mongo_find_reports_unavailable(mongo_reader: MongoEventReader) -> None:
    with pytest.raises(DependencyUnavailableError, match="MongoDB"):
        await mongo_reader.find(EventFilter(), 10, None)


async def test_mongo_stats_reports_unavailable(mongo_reader: MongoEventReader) -> None:
    with pytest.raises(DependencyUnavailableError, match="MongoDB"):
        await mongo_reader.count_by_bucket(EventFilter(), TimeBucket.DAY)


async def test_elasticsearch_search_reports_unavailable(
    search_index: ElasticsearchEventIndex,
) -> None:
    with pytest.raises(DependencyUnavailableError, match="Elasticsearch"):
        await search_index.search("firefox", EventFilter(), 10)


async def test_elasticsearch_index_reports_unavailable(
    search_index: ElasticsearchEventIndex,
) -> None:
    with pytest.raises(DependencyUnavailableError, match="Elasticsearch"):
        await search_index.index(make_event())
