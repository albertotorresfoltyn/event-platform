"""Mapping between the Event entity and its Elasticsearch document shape."""

from collections.abc import Iterator
from datetime import datetime
from typing import Any

from event_platform.domain.events import Event, Metadata

Source = dict[str, Any]


def to_document(event: Event) -> Source:
    return {
        "event_id": event.event_id,
        "event_type": event.event_type,
        "timestamp": event.timestamp.isoformat(),
        "user_id": event.user_id,
        "source_url": event.source_url,
        "metadata": event.metadata,
        "metadata_text": metadata_text(event.metadata),
    }


def from_source(source: Source) -> Event:
    return Event(
        event_id=source["event_id"],
        event_type=source["event_type"],
        timestamp=datetime.fromisoformat(source["timestamp"]),
        user_id=source["user_id"],
        source_url=source["source_url"],
        metadata=source.get("metadata", {}),
    )


def metadata_text(metadata: Metadata) -> str:
    """All leaf values of the metadata tree, space separated, for full-text search."""
    return " ".join(_leaf_values(metadata))


def _leaf_values(value: Any) -> Iterator[str]:
    if isinstance(value, dict):
        for nested in value.values():
            yield from _leaf_values(nested)
    elif isinstance(value, list):
        for item in value:
            yield from _leaf_values(item)
    elif value is not None:
        yield str(value)
