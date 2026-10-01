from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from event_platform.config import Settings
from event_platform.main import create_app


@pytest.fixture
def settings() -> Settings:
    return Settings(
        app_name="event-platform-test",
        log_level="WARNING",
        worker_enabled=False,
        rate_limit_enabled=False,
    )


@pytest.fixture
def client(settings: Settings) -> Iterator[TestClient]:
    with TestClient(create_app(settings)) as test_client:
        yield test_client
