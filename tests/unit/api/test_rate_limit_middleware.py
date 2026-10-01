from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from event_platform.api.middleware.rate_limit import RateLimitMiddleware
from event_platform.application.errors import DependencyUnavailableError
from event_platform.application.rate_limiting import RateLimitDecision, RateLimiter


class FakeRateLimiter:
    def __init__(self, *, limit: int = 2, available: bool = True) -> None:
        self.limit = limit
        self.available = available
        self.hits: dict[str, int] = {}

    async def hit(self, client_key: str) -> RateLimitDecision:
        if not self.available:
            raise DependencyUnavailableError("Redis")
        self.hits[client_key] = self.hits.get(client_key, 0) + 1
        count = self.hits[client_key]
        return RateLimitDecision(
            allowed=count <= self.limit,
            limit=self.limit,
            remaining=max(self.limit - count, 0),
            reset_after_seconds=30,
        )


def make_client(limiter: RateLimiter | None) -> TestClient:
    app = FastAPI()

    def provider(_: Request) -> RateLimiter | None:
        return limiter

    app.add_middleware(RateLimitMiddleware, limiter_provider=provider)

    @app.get("/events")
    async def events() -> dict[str, str]:
        return {"ok": "yes"}

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return TestClient(app)


def test_requests_within_the_limit_pass_with_rate_limit_headers() -> None:
    response = make_client(FakeRateLimiter(limit=2)).get("/events")

    assert response.status_code == 200
    assert response.headers["X-RateLimit-Limit"] == "2"
    assert response.headers["X-RateLimit-Remaining"] == "1"
    assert response.headers["X-RateLimit-Reset"] == "30"


def test_requests_over_the_limit_get_429_with_retry_after() -> None:
    client = make_client(FakeRateLimiter(limit=1))
    client.get("/events")

    response = client.get("/events")

    assert response.status_code == 429
    assert response.headers["Retry-After"] == "30"
    assert response.headers["X-RateLimit-Remaining"] == "0"


def test_limiter_outage_fails_open() -> None:
    response = make_client(FakeRateLimiter(available=False)).get("/events")

    assert response.status_code == 200
    assert "X-RateLimit-Limit" not in response.headers


def test_health_endpoints_are_exempt() -> None:
    limiter = FakeRateLimiter(limit=0)

    response = make_client(limiter).get("/health")

    assert response.status_code == 200
    assert limiter.hits == {}


def test_disabled_limiter_lets_everything_through() -> None:
    assert make_client(None).get("/events").status_code == 200
