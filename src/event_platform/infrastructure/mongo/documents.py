"""Mapping between the Event entity and its MongoDB document shape.

``event_id`` is stored as ``_id``: the primary key gives deduplication for free.
"""

from datetime import datetime
from typing import Any

from event_platform.domain.events import Event

Document = dict[str, Any]


def to_document(event: Event, ingested_at: datetime) -> Document:
    return {
        "_id": event.event_id,
        "event_type": event.event_type,
        "timestamp": event.timestamp,
        "user_id": event.user_id,
        "source_url": event.source_url,
        "metadata": event.metadata,
        "ingested_at": ingested_at,
    }
