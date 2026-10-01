"""Full request lifecycles: HTTP ingest -> queue -> worker -> MongoDB."""

from collections.abc import Iterator
from datetime import UTC, datetime

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


async def test_ingested_event_is_returned_by_the_query_endpoints(client: TestClient) -> None:
    payload = make_event_payload(
        event_type="conversion", user_id="user-query", timestamp="2026-10-01T10:30:00Z"
    )
    event_id = client.post("/events", json=payload).json()["event_id"]

    async def listed() -> bool:
        items = client.get("/events", params={"user_id": "user-query"}).json()["items"]
        return [item["event_id"] for item in items] == [event_id]

    await wait_until(listed)
    stats = client.get(
        "/events/stats",
        params={
            "bucket": "day",
            "event_type": "conversion",
            "start": "2026-10-01T00:00:00Z",
            "end": "2026-10-02T00:00:00Z",
        },
    ).json()
    assert stats["items"] == [
        {"bucket_start": "2026-10-01T00:00:00Z", "event_type": "conversion", "count": 1}
    ]


async def test_ingested_event_becomes_searchable(client: TestClient) -> None:
    payload = make_event_payload(metadata={"campaign": "Black Friday", "device": "tablet"})
    event_id = client.post("/events", json=payload).json()["event_id"]

    async def found() -> bool:
        body = client.get("/events/search", params={"q": "black friday tablet"}).json()
        return [item["event"]["event_id"] for item in body["items"]] == [event_id]

    await wait_until(found)


async def test_realtime_stats_reflect_ingested_events_after_cache_expiry(
    client: TestClient,
) -> None:
    assert client.get("/events/stats/realtime").json()["total"] == 0  # cached for 1s

    now = datetime.now(UTC).isoformat()
    client.post("/events", json=make_event_payload(event_type="signup", timestamp=now))

    async def counted() -> bool:
        body = client.get("/events/stats/realtime").json()
        return bool(body["counts_by_type"] == {"signup": 1})

    await wait_until(counted)


def test_readiness_is_ok_when_every_dependency_is_up(client: TestClient) -> None:
    response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
