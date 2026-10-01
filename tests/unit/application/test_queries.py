from datetime import UTC, datetime, timedelta, timezone

import pytest

from event_platform.application.errors import InvalidQueryError
from event_platform.application.queries import Cursor, EventFilter


def test_cursor_round_trips_through_its_token() -> None:
    cursor = Cursor(timestamp=datetime(2026, 10, 1, 12, 0, tzinfo=UTC), event_id="evt-1")

    assert Cursor.decode(cursor.encode()) == cursor


@pytest.mark.parametrize("token", ["not-base64!", "e30=", "eyJ0cyI6ICJub3BlIiwgImlkIjogIngifQ=="])
def test_malformed_cursor_is_rejected(token: str) -> None:
    with pytest.raises(InvalidQueryError, match="cursor"):
        Cursor.decode(token)


def test_filter_bounds_are_normalised_to_utc() -> None:
    event_filter = EventFilter(
        start=datetime(2026, 10, 1, 9, 0, tzinfo=timezone(timedelta(hours=-3))),
        end=datetime(2026, 10, 2, 0, 0),
    )

    assert event_filter.start == datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
    assert event_filter.end == datetime(2026, 10, 2, 0, 0, tzinfo=UTC)


def test_filter_rejects_inverted_range() -> None:
    with pytest.raises(InvalidQueryError, match="start must be earlier"):
        EventFilter(start=datetime(2026, 10, 2, tzinfo=UTC), end=datetime(2026, 10, 1, tzinfo=UTC))
