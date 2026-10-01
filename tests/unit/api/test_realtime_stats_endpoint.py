from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from event_platform.api.dependencies import get_realtime_stats_service
from event_platform.application.realtime_stats import RealtimeStatsService
from event_platform.config import Settings
from event_platform.main import create_app
from tests.fakes import FakeRealtimeStatsCache, StubEventReader

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
def client(settings: Settings) -> Iterator[TestClient]:
    service = RealtimeStatsService(
        StubEventReader(counts_by_type={"click": 2, "pageview": 5}),
        FakeRealtimeStatsCache(),
        window=timedelta(hours=1),
        ttl_seconds=10,
        clock=lambda: NOW,
    )
    app = create_app(settings)
    app.dependency_overrides[get_realtime_stats_service] = lambda: service
    with TestClient(app) as test_client:
        yield test_client


def test_first_request_is_a_miss_then_a_hit(client: TestClient) -> None:
    first = client.get("/events/stats/realtime")
    second = client.get("/events/stats/realtime")

    assert first.status_code == second.status_code == 200
    assert (first.headers["X-Cache"], second.headers["X-Cache"]) == ("MISS", "HIT")
    assert first.json() == {
        "generated_at": "2026-10-01T12:00:00Z",
        "window_start": "2026-10-01T11:00:00Z",
        "window_end": "2026-10-01T12:00:00Z",
        "total": 7,
        "counts_by_type": {"click": 2, "pageview": 5},
        "cache": {"hit": False, "ttl_seconds": 10},
    }
    assert second.json()["cache"]["hit"] is True
