"""Adapters must surface an outage as DependencyUnavailableError, not a driver error.

Pointed at a closed local port, so no running services are needed.
"""

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from elasticsearch import AsyncElasticsearch
from pymongo import AsyncMongoClient
from redis.asyncio import Redis

from event_platform.application.errors import DependencyUnavailableError
from event_platform.application.queries import EventFilter, RealtimeStats, TimeBucket
from event_platform.infrastructure.elasticsearch.event_index import ElasticsearchEventIndex
from event_platform.infrastructure.mongo.documents import Document
from event_platform.infrastructure.mongo.event_reader import MongoEventReader
from event_platform.infrastructure.redis.realtime_stats_cache import RedisRealtimeStatsCache
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


@pytest.fixture
async def stats_cache() -> AsyncIterator[RedisRealtimeStatsCache]:
    client = Redis.from_url(f"redis://{CLOSED_PORT_URL}", socket_connect_timeout=0.5)
    yield RedisRealtimeStatsCache(client, key_prefix="test", window_seconds=60)
    await client.aclose()


async def test_mongo_count_by_type_reports_unavailable(mongo_reader: MongoEventReader) -> None:
    with pytest.raises(DependencyUnavailableError, match="MongoDB"):
        await mongo_reader.count_by_type(EventFilter())


async def test_redis_get_reports_unavailable(stats_cache: RedisRealtimeStatsCache) -> None:
    with pytest.raises(DependencyUnavailableError, match="Redis"):
        await stats_cache.get()


async def test_redis_set_reports_unavailable(stats_cache: RedisRealtimeStatsCache) -> None:
    now = datetime.now(UTC)
    stats = RealtimeStats(now, now, now, 0, {})

    with pytest.raises(DependencyUnavailableError, match="Redis"):
        await stats_cache.set(stats, ttl_seconds=10)
