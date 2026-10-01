from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from event_platform.domain.errors import InvalidEventError
from event_platform.domain.events import MAX_ID_LENGTH, MAX_METADATA_BYTES
from tests.factories import make_event


def test_valid_event_is_created() -> None:
    event = make_event(event_type="conversion")

    assert event.event_type == "conversion"


def test_timestamp_is_normalised_to_utc() -> None:
    local = datetime(2026, 10, 1, 9, 0, tzinfo=timezone(timedelta(hours=-3)))

    event = make_event(timestamp=local)

    assert event.timestamp == datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
    assert event.timestamp.tzinfo is UTC


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"event_type": "Page View"}, "event_type"),
        ({"event_type": "1click"}, "event_type"),
        ({"event_type": "a" * 65}, "event_type"),
        ({"user_id": "   "}, "user_id must not be blank"),
        ({"user_id": "u" * (MAX_ID_LENGTH + 1)}, "user_id must be at most"),
        ({"event_id": ""}, "event_id must not be blank"),
        ({"source_url": "not-a-url"}, "source_url"),
        ({"source_url": "ftp://example.com/file"}, "source_url"),
        ({"source_url": "https://"}, "source_url"),
        ({"timestamp": datetime(2026, 10, 1, 12, 0)}, "timezone"),
        ({"metadata": {"blob": "x" * MAX_METADATA_BYTES}}, "metadata"),
    ],
)
def test_invalid_event_is_rejected(overrides: dict[str, Any], message: str) -> None:
    with pytest.raises(InvalidEventError, match=message):
        make_event(**overrides)
