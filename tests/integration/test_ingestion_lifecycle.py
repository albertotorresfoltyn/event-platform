"""Full request lifecycles: HTTP ingest -> queue -> worker -> MongoDB."""

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from pymongo.asynchronous.database import AsyncDatabase

from event_platform.config import Settings
from event_platform.infrastructure.mongo.documents import Document
from event_platform.main import create_app
from tests.factories import make_event_payload
from tests.polling import wait_until

pytestmark = pytest.mark.integration


@pytest.fixture
def client(integration_settings: Settings) -> Iterator[TestClient]:
    with TestClient(create_app(integration_settings)) as test_client:
        yield test_client


async def test_ingested_event_is_persisted_by_the_worker(
    client: TestClient, mongo_database: AsyncDatabase[Document]
) -> None:
    response = client.post("/events", json=make_event_payload(user_id="user-lifecycle"))
    event_id = response.json()["event_id"]

    async def persisted() -> bool:
        return await mongo_database["events"].find_one({"_id": event_id}) is not None

    await wait_until(persisted)
    document = await mongo_database["events"].find_one({"_id": event_id})
    assert document is not None
    assert document["user_id"] == "user-lifecycle"


async def test_resubmitted_event_is_stored_once(
    client: TestClient, mongo_database: AsyncDatabase[Document]
) -> None:
    payload = make_event_payload(event_id="evt-duplicate")
    for _ in range(3):
        assert client.post("/events", json=payload).status_code == 202

    async def queue_drained() -> bool:
        return bool(client.app.state.container.queue.depth == 0)  # type: ignore[attr-defined]

    await wait_until(queue_drained)
    assert await mongo_database["events"].count_documents({"_id": "evt-duplicate"}) == 1
