"""Integration fixtures backed by real services (``docker compose up -d``).

Each test gets its own throwaway MongoDB database and Elasticsearch index. Tests are
skipped, not failed, when the services are not reachable so the unit suite stays
runnable anywhere.
"""

import os
import uuid
from collections.abc import AsyncIterator

import pytest
from elasticsearch import AsyncElasticsearch, Elasticsearch
from elasticsearch import ConnectionError as EsConnectionError
from pymongo import AsyncMongoClient, MongoClient
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import PyMongoError

from event_platform.config import Settings
from event_platform.infrastructure.elasticsearch.event_index import ElasticsearchEventIndex
from event_platform.infrastructure.mongo.documents import Document

MONGO_URL = os.environ.get("TEST_MONGO_URL", "mongodb://localhost:27017")
ELASTICSEARCH_URL = os.environ.get("TEST_ELASTICSEARCH_URL", "http://localhost:9200")

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


@pytest.fixture(scope="session")
def elasticsearch_available() -> bool:
    client = Elasticsearch(ELASTICSEARCH_URL, request_timeout=1, max_retries=0)
    try:
        return bool(client.ping())
    except EsConnectionError:
        return False
    finally:
        client.close()


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
def index_name() -> str:
    return f"events_test_{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def elasticsearch_client(
    elasticsearch_available: bool, index_name: str
) -> AsyncIterator[AsyncElasticsearch]:
    if not elasticsearch_available:
        pytest.skip(f"Elasticsearch not reachable at {ELASTICSEARCH_URL}")
    client = AsyncElasticsearch(ELASTICSEARCH_URL)
    yield client
    await client.indices.delete(index=index_name, ignore_unavailable=True)
    await client.close()


@pytest.fixture
async def search_index(
    elasticsearch_client: AsyncElasticsearch, index_name: str
) -> ElasticsearchEventIndex:
    index = ElasticsearchEventIndex(elasticsearch_client, index_name)
    await index.ensure_index()
    return index


@pytest.fixture
def integration_settings(
    database_name: str,
    index_name: str,
    mongo_database: AsyncDatabase[Document],
    elasticsearch_client: AsyncElasticsearch,
) -> Settings:
    return Settings(
        log_level="WARNING",
        mongo_url=MONGO_URL,
        mongo_database=database_name,
        elasticsearch_url=ELASTICSEARCH_URL,
        elasticsearch_index=index_name,
        worker_poll_wait_seconds=0.05,
        retry_base_delay_seconds=0.05,
        retry_max_delay_seconds=0.1,
    )
