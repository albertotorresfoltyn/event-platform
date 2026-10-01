"""Integration fixtures backed by real services (``docker compose up -d``).

Each test gets its own throwaway MongoDB database. Tests are skipped, not failed,
when the services are not reachable so the unit suite stays runnable anywhere.
"""

import os
import uuid
from collections.abc import AsyncIterator

import pytest
from pymongo import AsyncMongoClient, MongoClient
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import PyMongoError

from event_platform.config import Settings
from event_platform.infrastructure.mongo.documents import Document

MONGO_URL = os.environ.get("TEST_MONGO_URL", "mongodb://localhost:27017")

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def mongo_available() -> bool:
    client: MongoClient[Document] = MongoClient(MONGO_URL, serverSelectionTimeoutMS=1_000)
    try:
        client.admin.command("ping")
    except PyMongoError:
        return False
    finally:
        client.close()
    return True


@pytest.fixture
def database_name() -> str:
    return f"event_platform_test_{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def mongo_database(
    mongo_available: bool, database_name: str
) -> AsyncIterator[AsyncDatabase[Document]]:
    if not mongo_available:
        pytest.skip(f"MongoDB not reachable at {MONGO_URL}")
    client: AsyncMongoClient[Document] = AsyncMongoClient(MONGO_URL, tz_aware=True)
    yield client[database_name]
    await client.drop_database(database_name)
    await client.close()


@pytest.fixture
def integration_settings(database_name: str, mongo_database: AsyncDatabase[Document]) -> Settings:
    return Settings(
        log_level="WARNING",
        mongo_url=MONGO_URL,
        mongo_database=database_name,
        worker_poll_wait_seconds=0.05,
        retry_base_delay_seconds=0.05,
        retry_max_delay_seconds=0.1,
    )
