from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from event_platform.api.dependencies import get_query_service
from event_platform.application.queries import Cursor, EventCount, TimeBucket
from event_platform.application.querying import EventQueryService
from event_platform.config import Settings
from event_platform.main import create_app
from tests.factories import make_event
from tests.fakes import StubEventReader

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
def reader() -> StubEventReader:
    return StubEventReader(
        events=[make_event(event_id="evt-2"), make_event(event_id="evt-1")],
        counts=[EventCount(bucket_start=NOW, event_type="click", count=3)],
    )


@pytest.fixture
def client(settings: Settings, reader: StubEventReader) -> Iterator[TestClient]:
    app = create_app(settings)
    app.dependency_overrides[get_query_service] = lambda: EventQueryService(
        reader, clock=lambda: NOW
    )
    with TestClient(app) as test_client:
        yield test_client


def test_list_events_returns_page_and_cursor(client: TestClient, reader: StubEventReader) -> None:
    response = client.get(
        "/events", params={"event_type": "pageview", "user_id": "user-123", "limit": 1}
    )

    assert response.status_code == 200
    body = response.json()
    assert [item["event_id"] for item in body["items"]] == ["evt-2"]
    assert Cursor.decode(body["next_cursor"]).event_id == "evt-2"
    event_filter, _, _ = reader.find_calls[0]
    assert (event_filter.event_type, event_filter.user_id) == ("pageview", "user-123")


def test_list_events_accepts_cursor_from_previous_page(
    client: TestClient, reader: StubEventReader
) -> None:
    token = Cursor(NOW, "evt-2").encode()

    client.get("/events", params={"cursor": token})

    assert reader.find_calls[0][2] == Cursor(NOW, "evt-2")


@pytest.mark.parametrize(
    "params",
    [
        {"limit": 0},
        {"limit": 501},
        {"cursor": "garbage"},
        {"start": "2026-10-02T00:00:00Z", "end": "2026-10-01T00:00:00Z"},
        {"start": "not-a-date"},
    ],
)
def test_list_events_rejects_invalid_parameters(
    client: TestClient, params: dict[str, object]
) -> None:
    assert client.get("/events", params=params).status_code == 422


def test_stats_returns_counts_and_resolved_window(
    client: TestClient, reader: StubEventReader
) -> None:
    response = client.get("/events/stats", params={"bucket": "hour"})

    assert response.status_code == 200
    body = response.json()
    assert body["bucket"] == "hour"
    assert body["end"] == "2026-10-01T12:00:00Z"
    assert body["items"] == [
        {"bucket_start": "2026-10-01T12:00:00Z", "event_type": "click", "count": 3}
    ]
    assert reader.count_calls[0][1] is TimeBucket.HOUR


@pytest.mark.parametrize(
    "params",
    [{"bucket": "minute"}, {"bucket": "hour", "start": "2020-01-01T00:00:00Z"}],
)
def test_stats_rejects_invalid_parameters(client: TestClient, params: dict[str, object]) -> None:
    assert client.get("/events/stats", params=params).status_code == 422
