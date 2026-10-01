from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from event_platform.api.dependencies import get_query_service, get_search_service
from event_platform.application.errors import DependencyUnavailableError
from event_platform.application.queries import SearchHit, SearchResult
from event_platform.application.search import EventSearchService
from event_platform.config import Settings
from event_platform.main import create_app
from tests.factories import make_event
from tests.fakes import StubEventSearcher


@pytest.fixture
def searcher() -> StubEventSearcher:
    hit = SearchHit(event=make_event(event_id="evt-1"), score=2.5)
    return StubEventSearcher(SearchResult(total=1, hits=[hit]))


@pytest.fixture
def client(settings: Settings, searcher: StubEventSearcher) -> Iterator[TestClient]:
    app = create_app(settings)
    app.dependency_overrides[get_search_service] = lambda: EventSearchService(searcher)
    with TestClient(app) as test_client:
        yield test_client


def test_search_returns_scored_hits(client: TestClient, searcher: StubEventSearcher) -> None:
    response = client.get("/events/search", params={"q": "firefox", "event_type": "click"})

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["score"] == 2.5
    assert body["items"][0]["event"]["event_id"] == "evt-1"
    text, event_filter, limit = searcher.calls[0]
    assert (text, event_filter.event_type, limit) == ("firefox", "click", 20)


@pytest.mark.parametrize("params", [{}, {"q": ""}, {"q": "   "}, {"q": "x", "limit": 101}])
def test_search_rejects_invalid_parameters(client: TestClient, params: dict[str, object]) -> None:
    assert client.get("/events/search", params=params).status_code == 422


def test_unavailable_dependency_returns_503(settings: Settings) -> None:
    class DownService:
        async def list_events(self, *_: object, **__: object) -> None:
            raise DependencyUnavailableError("MongoDB")

    app = create_app(settings)
    app.dependency_overrides[get_query_service] = DownService
    with TestClient(app) as client:
        response = client.get("/events")

    assert response.status_code == 503
    assert response.json() == {"detail": "MongoDB is unavailable, retry later"}
    assert response.headers["Retry-After"] == "5"
