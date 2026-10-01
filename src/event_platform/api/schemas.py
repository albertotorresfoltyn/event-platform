"""HTTP request/response contracts. Business rules live in the domain, not here."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

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
