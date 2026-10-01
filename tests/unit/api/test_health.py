from fastapi.testclient import TestClient

from event_platform.api.dependencies import get_readiness_service
from event_platform.application.health import ReadinessService
from event_platform.config import Settings
from event_platform.main import create_app


def test_liveness_returns_ok(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def _ok() -> None:
    return None


async def _down() -> None:
    raise ConnectionError


def ready_client(settings: Settings, service: ReadinessService) -> TestClient:
    app = create_app(settings)
    app.dependency_overrides[get_readiness_service] = lambda: service
    return TestClient(app)


def test_readiness_reports_degraded_dependencies_with_200(settings: Settings) -> None:
    service = ReadinessService(
        {"queue": _ok, "mongodb": _down}, critical=frozenset({"queue"}), timeout_seconds=1
    )

    with ready_client(settings, service) as client:
        response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "degraded",
        "components": {
            "queue": {"healthy": True, "detail": None},
            "mongodb": {"healthy": False, "detail": "ConnectionError"},
        },
    }


def test_readiness_is_503_when_queue_is_full(settings: Settings) -> None:
    service = ReadinessService({"queue": _down}, critical=frozenset({"queue"}), timeout_seconds=1)

    with ready_client(settings, service) as client:
        response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["status"] == "unavailable"
