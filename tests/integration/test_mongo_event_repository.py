from datetime import UTC, datetime

import pytest
from pymongo.asynchronous.database import AsyncDatabase

from event_platform.infrastructure.mongo.documents import Document
from event_platform.infrastructure.mongo.event_repository import MongoEventRepository
from tests.factories import make_event

pytestmark = pytest.mark.integration

INGESTED_AT = datetime(2026, 10, 1, 12, 0, 5, tzinfo=UTC)


@pytest.fixture
def repository(mongo_database: AsyncDatabase[Document]) -> MongoEventRepository:
    return MongoEventRepository(mongo_database["events"], clock=lambda: INGESTED_AT)


async def test_save_stores_event_keyed_by_event_id(
    repository: MongoEventRepository, mongo_database: AsyncDatabase[Document]
) -> None:
    event = make_event()

    assert await repository.save(event) is True

    document = await mongo_database["events"].find_one({"_id": event.event_id})
    assert document == {
        "_id": event.event_id,
        "event_type": event.event_type,
        "timestamp": event.timestamp,
        "user_id": event.user_id,
        "source_url": event.source_url,
        "metadata": event.metadata,
        "ingested_at": INGESTED_AT,
    }


async def test_saving_same_event_twice_is_idempotent(
    repository: MongoEventRepository, mongo_database: AsyncDatabase[Document]
) -> None:
    event = make_event()

    assert await repository.save(event) is True
    assert await repository.save(event) is False
    assert await mongo_database["events"].count_documents({}) == 1
