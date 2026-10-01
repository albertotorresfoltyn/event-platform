"""MongoDB implementation of the EventReader port."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import ConnectionFailure

from event_platform.application.errors import DependencyUnavailableError
from event_platform.application.queries import Cursor, EventCount, EventFilter, TimeBucket
from event_platform.domain.events import Event
from event_platform.infrastructure.mongo.documents import Document, from_document
from event_platform.infrastructure.mongo.queries import (
    NEWEST_FIRST,
    build_count_by_bucket_pipeline,
    build_count_by_type_pipeline,
    build_find_query,
)


@asynccontextmanager
async def _translate_errors() -> AsyncIterator[None]:
    try:
        yield
    except ConnectionFailure as exc:  # includes server selection timeouts
        raise DependencyUnavailableError("MongoDB") from exc


class MongoEventReader:
    def __init__(self, collection: AsyncCollection[Document]) -> None:
        self._collection = collection

    async def find(
        self, event_filter: EventFilter, limit: int, after: Cursor | None
    ) -> list[Event]:
        async with _translate_errors():
            cursor = (
                self._collection.find(build_find_query(event_filter, after))
                .sort(NEWEST_FIRST)
                .limit(limit)
            )
            return [from_document(document) async for document in cursor]

    async def count_by_bucket(
        self, event_filter: EventFilter, bucket: TimeBucket
    ) -> list[EventCount]:
        pipeline = build_count_by_bucket_pipeline(event_filter, bucket)
        async with _translate_errors():
            results = await self._collection.aggregate(pipeline)
            return [
                EventCount(
                    bucket_start=row["_id"]["bucket_start"],
                    event_type=row["_id"]["event_type"],
                    count=row["count"],
                )
                async for row in results
            ]

    async def count_by_type(self, event_filter: EventFilter) -> dict[str, int]:
        pipeline = build_count_by_type_pipeline(event_filter)
        async with _translate_errors():
            results = await self._collection.aggregate(pipeline)
            return {row["_id"]: row["count"] async for row in results}
