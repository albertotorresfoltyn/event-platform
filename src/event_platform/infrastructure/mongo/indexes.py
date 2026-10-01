"""MongoDB index definitions, derived from the read patterns of the API.

Every list query sorts newest-first by (timestamp, _id), so each filterable field
gets a compound index with the equality field first and the sort keys after it
(the ESR rule: Equality, Sort, Range). That lets MongoDB resolve filter, date
range and sort from the index alone, with no in-memory sort.

See ARCHITECTURE.md for indexes deliberately *not* created.
"""

from pymongo import ASCENDING, DESCENDING, IndexModel
from pymongo.asynchronous.collection import AsyncCollection

from event_platform.infrastructure.mongo.documents import Document

_SORT_KEYS = [("timestamp", DESCENDING), ("_id", DESCENDING)]

EVENT_INDEXES = [
    # Unfiltered listing, date-range-only listing and the stats $match on a window.
    IndexModel(_SORT_KEYS, name="timestamp_id"),
    # Filter by type (+ range); also serves stats requests filtered by event type.
    IndexModel([("event_type", ASCENDING), *_SORT_KEYS], name="event_type_timestamp_id"),
    # A user's activity timeline.
    IndexModel([("user_id", ASCENDING), *_SORT_KEYS], name="user_id_timestamp_id"),
    # Events on a given page.
    IndexModel([("source_url", ASCENDING), *_SORT_KEYS], name="source_url_timestamp_id"),
]


async def ensure_indexes(collection: AsyncCollection[Document]) -> None:
    """Idempotent: createIndexes is a no-op for indexes that already exist."""
    await collection.create_indexes(EVENT_INDEXES)
