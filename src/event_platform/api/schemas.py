"""HTTP request/response contracts. Business rules live in the domain, not here."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from event_platform.application.queries import (
    EventPage,
    EventStats,
    SearchResult,
    TimeBucket,
)
from event_platform.domain.events import Event, new_event_id


class EventIn(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "event_type": "pageview",
                "timestamp": "2026-10-01T12:00:00Z",
                "user_id": "user-123",
                "source_url": "https://example.com/pricing",
                "metadata": {"browser": "firefox", "device": "desktop"},
            }
        },
    )

    event_id: str | None = Field(
        default=None, description="Optional idempotency key; generated when omitted."
    )
    event_type: str
    timestamp: datetime
    user_id: str
    source_url: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_domain(self) -> Event:
        return Event(
            event_id=self.event_id or new_event_id(),
            event_type=self.event_type,
            timestamp=self.timestamp,
            user_id=self.user_id,
            source_url=self.source_url,
            metadata=self.metadata,
        )


class EventAccepted(BaseModel):
    event_id: str
    status: Literal["queued"] = "queued"


class EventOut(BaseModel):
    event_id: str
    event_type: str
    timestamp: datetime
    user_id: str
    source_url: str
    metadata: dict[str, Any]

    @classmethod
    def from_domain(cls, event: Event) -> "EventOut":
        return cls(
            event_id=event.event_id,
            event_type=event.event_type,
            timestamp=event.timestamp,
            user_id=event.user_id,
            source_url=event.source_url,
            metadata=event.metadata,
        )


class EventPageOut(BaseModel):
    items: list[EventOut]
    next_cursor: str | None = Field(description="Pass as `cursor` to fetch the next page.")

    @classmethod
    def from_domain(cls, page: EventPage) -> "EventPageOut":
        return cls(
            items=[EventOut.from_domain(event) for event in page.items],
            next_cursor=page.next_cursor.encode() if page.next_cursor else None,
        )


class EventCountOut(BaseModel):
    bucket_start: datetime
    event_type: str
    count: int


class EventStatsOut(BaseModel):
    bucket: TimeBucket
    start: datetime
    end: datetime
    items: list[EventCountOut]

    @classmethod
    def from_domain(cls, stats: EventStats) -> "EventStatsOut":
        return cls(
            bucket=stats.bucket,
            start=stats.start,
            end=stats.end,
            items=[
                EventCountOut(bucket_start=c.bucket_start, event_type=c.event_type, count=c.count)
                for c in stats.counts
            ],
        )


class SearchHitOut(BaseModel):
    score: float
    event: EventOut


class SearchResultOut(BaseModel):
    total: int = Field(description="Matching events; counted exactly up to 10,000.")
    items: list[SearchHitOut]

    @classmethod
    def from_domain(cls, result: SearchResult) -> "SearchResultOut":
        return cls(
            total=result.total,
            items=[
                SearchHitOut(score=hit.score, event=EventOut.from_domain(hit.event))
                for hit in result.hits
            ],
        )
