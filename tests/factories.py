"""Test data builders shared by unit and integration tests."""

from datetime import UTC, datetime
from typing import Any

from event_platform.domain.events import Event, new_event_id


def make_event(**overrides: Any) -> Event:
    fields: dict[str, Any] = {
        "event_id": new_event_id(),
        "event_type": "pageview",
        "timestamp": datetime(2026, 10, 1, 12, 0, tzinfo=UTC),
        "user_id": "user-123",
        "source_url": "https://example.com/pricing",
        "metadata": {"browser": "firefox", "device": "desktop"},
    }
    return Event(**(fields | overrides))


def make_event_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event_type": "pageview",
        "timestamp": "2026-10-01T12:00:00Z",
        "user_id": "user-123",
        "source_url": "https://example.com/pricing",
        "metadata": {"browser": "firefox", "device": "desktop"},
    }
    return payload | overrides
