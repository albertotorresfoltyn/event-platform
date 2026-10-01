"""Dependency container: the one place where concrete adapters are chosen and owned."""

from dataclasses import dataclass

from pymongo import AsyncMongoClient

from event_platform.application.backoff import ExponentialBackoff
from event_platform.application.ingestion import EventIngestionService
from event_platform.application.processing import EventProcessor
from event_platform.application.worker import EventWorker
from event_platform.config import Settings
from event_platform.infrastructure.mongo.documents import Document
from event_platform.infrastructure.mongo.event_repository import MongoEventRepository
from event_platform.infrastructure.queue.in_memory import InMemoryEventQueue


@dataclass(frozen=True, slots=True)
class Container:
    settings: Settings
    mongo_client: AsyncMongoClient[Document]
    queue: InMemoryEventQueue
    repository: MongoEventRepository
    ingestion_service: EventIngestionService
    worker: EventWorker

    async def start(self) -> None:
        if self.settings.worker_enabled:
            self.worker.start()

    async def stop(self) -> None:
        await self.worker.stop()
        await self.mongo_client.close()


def build_container(settings: Settings) -> Container:
    mongo_client: AsyncMongoClient[Document] = AsyncMongoClient(
        settings.mongo_url,
        tz_aware=True,
        serverSelectionTimeoutMS=settings.mongo_server_selection_timeout_ms,
    )
    collection = mongo_client[settings.mongo_database][settings.mongo_events_collection]
    repository = MongoEventRepository(collection)

    queue = InMemoryEventQueue(
        max_size=settings.queue_max_size,
        visibility_timeout_seconds=settings.queue_visibility_timeout_seconds,
        max_receive_count=settings.queue_max_receive_count,
    )
    processor = EventProcessor(
        repository=repository,
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
    return Container(
        settings=settings,
        mongo_client=mongo_client,
        queue=queue,
        repository=repository,
        ingestion_service=EventIngestionService(queue),
        worker=worker,
    )
