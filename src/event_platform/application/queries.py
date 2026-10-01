"""Read-side models: filters, keyset pagination and aggregation buckets."""

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from event_platform.application.errors import InvalidQueryError
from event_platform.domain.events import Event


def as_utc(value: datetime) -> datetime:
    """Query bounds without an offset are interpreted as UTC (ingestion is stricter)."""
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class EventFilter:
    event_type: str | None = None
    user_id: str | None = None
    source_url: str | None = None
    start: datetime | None = None
    end: datetime | None = None

    def __post_init__(self) -> None:
        if self.start is not None:
            object.__setattr__(self, "start", as_utc(self.start))
        if self.end is not None:
            object.__setattr__(self, "end", as_utc(self.end))
        if self.start and self.end and self.start >= self.end:
            raise InvalidQueryError("start must be earlier than end")


@dataclass(frozen=True, slots=True)
class Cursor:
    """Position after the last returned event, ordered by (timestamp, event_id) desc.

    Keyset pagination stays O(page size) on deep pages, unlike skip/offset, and is
    stable while new events are being inserted.
    """

    timestamp: datetime
    event_id: str

    def encode(self) -> str:
        payload = json.dumps({"ts": self.timestamp.isoformat(), "id": self.event_id})
        return base64.urlsafe_b64encode(payload.encode()).decode()

    @classmethod
    def decode(cls, token: str) -> "Cursor":
        try:
            payload = json.loads(base64.urlsafe_b64decode(token.encode()))
            return cls(
                timestamp=as_utc(datetime.fromisoformat(payload["ts"])), event_id=payload["id"]
            )
        except (binascii.Error, ValueError, KeyError, TypeError) as exc:
            raise InvalidQueryError("cursor is malformed") from exc


@dataclass(frozen=True, slots=True)
class EventPage:
    items: list[Event]
    next_cursor: Cursor | None


class TimeBucket(StrEnum):
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"

    @property
    def duration(self) -> timedelta:
        return {
            TimeBucket.HOUR: timedelta(hours=1),
            TimeBucket.DAY: timedelta(days=1),
            TimeBucket.WEEK: timedelta(weeks=1),
        }[self]


@dataclass(frozen=True, slots=True)
class EventCount:
    bucket_start: datetime
    event_type: str
    count: int


@dataclass(frozen=True, slots=True)
class EventStats:
    bucket: TimeBucket
    start: datetime
    end: datetime
    counts: list[EventCount]


@dataclass(frozen=True, slots=True)
class SearchHit:
    event: Event
    score: float


@dataclass(frozen=True, slots=True)
class SearchResult:
    total: int
    hits: list[SearchHit]
