"""MongoDB implementation of the EventReader port."""

from pymongo.asynchronous.collection import AsyncCollection

from event_platform.application.queries import Cursor, EventCount, EventFilter, TimeBucket
from event_platform.domain.events import Event
from event_platform.infrastructure.mongo.documents import Document, from_document
from event_platform.infrastructure.mongo.queries import (
    NEWEST_FIRST,
    build_count_by_bucket_pipeline,
    build_find_query,
)


class MongoEventReader:
    def __init__(self, collection: AsyncCollection[Document]) -> None:
        self._collection = collection

    async def find(
        self, event_filter: EventFilter, limit: int, after: Cursor | None
    ) -> list[Event]:
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
        results = await self._collection.aggregate(pipeline)
        return [
            EventCount(
                bucket_start=row["_id"]["bucket_start"],
                event_type=row["_id"]["event_type"],
                count=row["count"],
            )
            async for row in results
        ]
