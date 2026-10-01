"""MongoDB implementation of the EventRepository port."""

from collections.abc import Callable
from datetime import UTC, datetime

from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import DuplicateKeyError

from event_platform.domain.events import Event
from event_platform.infrastructure.mongo.documents import Document, to_document


def _utc_now() -> datetime:
    return datetime.now(UTC)


class MongoEventRepository:
    def __init__(
        self,
        collection: AsyncCollection[Document],
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._collection = collection
        self._clock = clock

    async def save(self, event: Event) -> bool:
        try:
            await self._collection.insert_one(to_document(event, ingested_at=self._clock()))
        except DuplicateKeyError:
            return False
        return True
