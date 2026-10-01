from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from event_platform.config import Settings
from event_platform.container import Container
from event_platform.main import create_app
from tests.factories import make_event

ADMIN_KEY = "s3cret"


@pytest.fixture
def admin_client(settings: Settings) -> Iterator[TestClient]:
    configured = settings.model_copy(
        update={"admin_api_key": SecretStr(ADMIN_KEY), "queue_max_receive_count": 1}
    )
    with TestClient(create_app(configured)) as client:
        yield client


async def dead_letter_one_event(client: TestClient) -> str:
    container: Container = client.app.state.container  # type: ignore[attr-defined]
    event = make_event()
    await container.queue.publish(event)
    [message] = await container.queue.receive(max_messages=1, wait_seconds=0)
    await container.queue.retry_later(message.receipt_handle, delay_seconds=0)
    await container.queue.receive(max_messages=1, wait_seconds=0)  # exceeds max receives
    return event.event_id


def test_admin_routes_are_hidden_without_a_configured_key(client: TestClient) -> None:
    assert client.get("/admin/dead-letters").status_code == 404


@pytest.mark.parametrize("headers", [{}, {"X-Admin-Key": "wrong"}])
def test_admin_routes_reject_missing_or_wrong_key(
    admin_client: TestClient, headers: dict[str, str]
) -> None:
    assert admin_client.get("/admin/dead-letters", headers=headers).status_code == 401


async def test_dead_letters_can_be_listed_and_redriven(admin_client: TestClient) -> None:
    event_id = await dead_letter_one_event(admin_client)
    headers = {"X-Admin-Key": ADMIN_KEY}

    listed = admin_client.get("/admin/dead-letters", headers=headers).json()
    redriven = admin_client.post("/admin/dead-letters/redrive", headers=headers).json()
    after = admin_client.get("/admin/dead-letters", headers=headers).json()

    assert listed["count"] == 1
    assert listed["items"][0]["event_id"] == event_id
    assert redriven == {"redriven": 1}
    assert after == {"count": 0, "items": []}
