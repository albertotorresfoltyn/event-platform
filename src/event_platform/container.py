"""Dependency container: the one place where concrete adapters are chosen and owned."""

import asyncio
import logging
from collections.abc import Coroutine
from dataclasses import dataclass, field
from datetime import timedelta

from elasticsearch import AsyncElasticsearch
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
from redis.asyncio import Redis

from event_platform.application.backoff import ExponentialBackoff
from event_platform.application.errors import QueueFullError
from event_platform.application.health import ReadinessService
from event_platform.application.ingestion import EventIngestionService
from event_platform.application.processing import EventProcessor
from event_platform.application.querying import EventQueryService
from event_platform.application.realtime_stats import RealtimeStatsService
from event_platform.application.search import EventSearchService
from event_platform.application.worker import EventWorker
from event_platform.config import Settings
from event_platform.infrastructure.elasticsearch.event_index import ElasticsearchEventIndex
from event_platform.infrastructure.mongo.documents import Document
from event_platform.infrastructure.mongo.event_reader import MongoEventReader
from event_platform.infrastructure.mongo.event_repository import MongoEventRepository
from event_platform.infrastructure.mongo.indexes import ensure_indexes
from event_platform.infrastructure.queue.in_memory import InMemoryEventQueue
from event_platform.infrastructure.redis.rate_limiter import RedisFixedWindowRateLimiter
from event_platform.infrastructure.redis.realtime_stats_cache import RedisRealtimeStatsCache

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Container:
    settings: Settings
    mongo_client: AsyncMongoClient[Document]
    elasticsearch_client: AsyncElasticsearch
    search_index: ElasticsearchEventIndex
    redis_client: Redis
    queue: InMemoryEventQueue
    repository: MongoEventRepository
    ingestion_service: EventIngestionService
    query_service: EventQueryService
    search_service: EventSearchService
    realtime_stats_service: RealtimeStatsService
    readiness_service: ReadinessService
    rate_limiter: RedisFixedWindowRateLimiter | None
    worker: EventWorker
    _background_tasks: set[asyncio.Task[None]] = field(default_factory=set)

    async def start(self) -> None:
        # Index creation must not block startup: if MongoDB is down the API can still
        # accept events into the queue, and the worker will retry until it recovers.
        self._spawn(self._ensure_indexes())
        self._spawn(self._ensure_search_index())
        if self.settings.worker_enabled:
            self.worker.start()

    async def stop(self) -> None:
        await self.worker.stop()
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.mongo_client.close()
        await self.elasticsearch_client.close()
        await self.redis_client.aclose()

    def _spawn(self, coroutine: Coroutine[None, None, None]) -> None:
        task = asyncio.create_task(coroutine)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _ensure_indexes(self) -> None:
        try:
            await ensure_indexes(self._events_collection)
            logger.info("MongoDB indexes ensured")
        except Exception:
            logger.exception("Could not ensure MongoDB indexes; queries may be slow")

    async def _ensure_search_index(self) -> None:
        try:
            await self.search_index.ensure_index()
        except Exception:
            logger.exception("Could not ensure Elasticsearch index; search may be unavailable")

    @property
    def _events_collection(self) -> AsyncCollection[Document]:
        database = self.mongo_client[self.settings.mongo_database]
        return database[self.settings.mongo_events_collection]


def build_container(settings: Settings) -> Container:
    mongo_client: AsyncMongoClient[Document] = AsyncMongoClient(
        settings.mongo_url,
        tz_aware=True,
        serverSelectionTimeoutMS=settings.mongo_server_selection_timeout_ms,
    )
    collection = mongo_client[settings.mongo_database][settings.mongo_events_collection]
    repository = MongoEventRepository(collection)

    elasticsearch_client = AsyncElasticsearch(
        settings.elasticsearch_url,
        request_timeout=settings.elasticsearch_request_timeout_seconds,
    )
    search_index = ElasticsearchEventIndex(
        elasticsearch_client,
        settings.elasticsearch_index,
        number_of_shards=settings.elasticsearch_shards,
        number_of_replicas=settings.elasticsearch_replicas,
    )

    redis_client = Redis.from_url(
        settings.redis_url,
        # Fail fast: a slow cache is worse than no cache, since we fall back to MongoDB.
        socket_timeout=settings.redis_socket_timeout_seconds,
        socket_connect_timeout=settings.redis_socket_timeout_seconds,
    )
    reader = MongoEventReader(collection)
    realtime_stats_service = RealtimeStatsService(
        reader,
        RedisRealtimeStatsCache(
            redis_client,
            key_prefix=settings.redis_key_prefix,
            window_seconds=settings.realtime_stats_window_seconds,
        ),
        window=timedelta(seconds=settings.realtime_stats_window_seconds),
        ttl_seconds=settings.realtime_stats_ttl_seconds,
    )

    queue = InMemoryEventQueue(
        max_size=settings.queue_max_size,
        visibility_timeout_seconds=settings.queue_visibility_timeout_seconds,
        max_receive_count=settings.queue_max_receive_count,
    )
    processor = EventProcessor(
        repository=repository,
        indexer=search_index,
        consumer=queue,
        backoff=ExponentialBackoff(
            base_seconds=settings.retry_base_delay_seconds,
            max_seconds=settings.retry_max_delay_seconds,
        ),
    )
    worker = EventWorker(
        consumer=queue,
        processor=processor,
        batch_size=settings.worker_batch_size,
        poll_wait_seconds=settings.worker_poll_wait_seconds,
    )

    async def check_mongo() -> None:
        await mongo_client.admin.command("ping")

    async def check_elasticsearch() -> None:
        if not await elasticsearch_client.ping():
            raise ConnectionError("Elasticsearch ping failed")

    async def check_redis() -> None:
        await redis_client.ping()

    async def check_queue() -> None:
        if queue.is_full:
            raise QueueFullError("ingestion queue is full")

    readiness_service = ReadinessService(
        {
            "queue": check_queue,
            "mongodb": check_mongo,
            "elasticsearch": check_elasticsearch,
            "redis": check_redis,
        },
        critical=frozenset({"queue"}),
        timeout_seconds=settings.readiness_check_timeout_seconds,
    )
    rate_limiter = (
        RedisFixedWindowRateLimiter(
            redis_client,
            key_prefix=settings.redis_key_prefix,
            limit=settings.rate_limit_requests,
            window_seconds=settings.rate_limit_window_seconds,
        )
        if settings.rate_limit_enabled
        else None
    )

    return Container(
        settings=settings,
        mongo_client=mongo_client,
        elasticsearch_client=elasticsearch_client,
        search_index=search_index,
        redis_client=redis_client,
        queue=queue,
        repository=repository,
        ingestion_service=EventIngestionService(queue),
        query_service=EventQueryService(reader),
        search_service=EventSearchService(search_index),
        realtime_stats_service=realtime_stats_service,
        readiness_service=readiness_service,
        rate_limiter=rate_limiter,
        worker=worker,
    )
