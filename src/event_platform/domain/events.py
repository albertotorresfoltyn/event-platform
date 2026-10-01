"""The Event entity: the single source of truth for what a valid event is."""

import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from event_platform.domain.errors import InvalidEventError

EVENT_TYPE_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,63}")
MAX_ID_LENGTH = 128
MAX_URL_LENGTH = 2048
MAX_METADATA_BYTES = 16 * 1024
ALLOWED_URL_SCHEMES = frozenset({"http", "https"})

Metadata = dict[str, Any]


def new_event_id() -> str:
    return uuid.uuid4().hex


@dataclass(frozen=True, slots=True)
class Event:
    """A single web event.

    ``event_id`` doubles as the idempotency key: producers may supply their own so
    that retried submissions are deduplicated downstream.
    """

    event_id: str
    event_type: str
    timestamp: datetime
    user_id: str
    source_url: str
    metadata: Metadata = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_bounded_text("event_id", self.event_id, MAX_ID_LENGTH)
        _require_bounded_text("user_id", self.user_id, MAX_ID_LENGTH)
        _require_event_type(self.event_type)
        _require_http_url(self.source_url)
        _require_metadata_size(self.metadata)
        object.__setattr__(self, "timestamp", _to_utc(self.timestamp))


def _require_bounded_text(name: str, value: str, max_length: int) -> None:
    if not value.strip():
        raise InvalidEventError(f"{name} must not be blank")
    if len(value) > max_length:
        raise InvalidEventError(f"{name} must be at most {max_length} characters")


def _require_event_type(event_type: str) -> None:
    if not EVENT_TYPE_PATTERN.fullmatch(event_type):
        raise InvalidEventError(
            "event_type must be lowercase snake_case, start with a letter and be at most 64 chars"
        )


def _require_http_url(url: str) -> None:
    parsed = urlparse(url)
    if len(url) > MAX_URL_LENGTH or parsed.scheme not in ALLOWED_URL_SCHEMES or not parsed.netloc:
        raise InvalidEventError("source_url must be an absolute http(s) URL")


def _require_metadata_size(metadata: Metadata) -> None:
    if len(json.dumps(metadata, default=str).encode()) > MAX_METADATA_BYTES:
        raise InvalidEventError(f"metadata must be at most {MAX_METADATA_BYTES} bytes")


def _to_utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        raise InvalidEventError("timestamp must include a timezone offset")
    return timestamp.astimezone(UTC)
