from typing import Any

import pytest
from fastapi.testclient import TestClient

from event_platform.api.dependencies import get_ingestion_service
from event_platform.config import Settings
from event_platform.container import Container
from event_platform.main import create_app
from tests.factories import make_event_payload


def container_of(client: TestClient) -> Container:
    container: Container = client.app.state.container  # type: ignore[attr-defined]
    return container


def test_valid_event_is_accepted_and_enqueued(client: TestClient) -> None:
    response = client.post("/events", json=make_event_payload())

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "queued"
    assert body["event_id"]
    assert container_of(client).queue.depth == 1


def test_client_supplied_event_id_is_preserved(client: TestClient) -> None:
    response = client.post("/events", json=make_event_payload(event_id="evt-42"))

    assert response.json()["event_id"] == "evt-42"


@pytest.mark.parametrize(
    "payload",
    [
        make_event_payload(event_type="Not Valid"),
        make_event_payload(source_url="javascript:alert(1)"),
        make_event_payload(timestamp="2026-10-01T12:00:00"),
        make_event_payload(timestamp="yesterday"),
        make_event_payload(unexpected="field"),
        {k: v for k, v in make_event_payload().items() if k != "user_id"},
    ],
)
def test_invalid_event_returns_422_and_is_not_enqueued(
    client: TestClient, payload: dict[str, Any]
) -> None:
    response = client.post("/events", json=payload)

    assert response.status_code == 422
    assert container_of(client).queue.depth == 0


def test_full_queue_returns_503_with_retry_after(settings: Settings) -> None:
    with TestClient(create_app(settings.model_copy(update={"queue_max_size": 1}))) as client:
        client.post("/events", json=make_event_payload())

        response = client.post("/events", json=make_event_payload())

    assert response.status_code == 503
    assert response.headers["Retry-After"] == "1"


def test_unexpected_error_returns_generic_500(settings: Settings) -> None:
    class BrokenService:
        async def ingest(self, _: object) -> None:
            raise RuntimeError("boom")

    app = create_app(settings)
    app.dependency_overrides[get_ingestion_service] = BrokenService
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post("/events", json=make_event_payload())

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error"}
