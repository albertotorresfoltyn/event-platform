"""Event endpoints: ingestion (write path) and querying (read path)."""

from typing import Annotated

from fastapi import APIRouter, Query, status

from event_platform.api.dependencies import (
    EventFilterDep,
    IngestionServiceDep,
    QueryServiceDep,
    SearchServiceDep,
)
from event_platform.api.schemas import (
    EventAccepted,
    EventIn,
    EventPageOut,
    EventStatsOut,
    SearchResultOut,
)
from event_platform.application.queries import Cursor, TimeBucket

router = APIRouter(prefix="/events", tags=["events"])

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 500
MAX_SEARCH_RESULTS = 100


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        422: {"description": "Event failed validation"},
        503: {"description": "Ingestion queue is full; retry after the Retry-After header"},
    },
)
async def ingest_event(payload: EventIn, service: IngestionServiceDep) -> EventAccepted:
    """Validate an event and enqueue it for asynchronous persistence."""
    event = payload.to_domain()
    await service.ingest(event)
    return EventAccepted(event_id=event.event_id)


@router.get("")
async def list_events(
    event_filter: EventFilterDep,
    service: QueryServiceDep,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
    cursor: Annotated[str | None, Query(description="Opaque `next_cursor` value")] = None,
) -> EventPageOut:
    """List events newest first, filtered by type, user, source URL and date range."""
    after = Cursor.decode(cursor) if cursor else None
    page = await service.list_events(event_filter, limit=limit, cursor=after)
    return EventPageOut.from_domain(page)


@router.get("/search", responses={503: {"description": "Elasticsearch is unavailable"}})
async def search_events(
    q: Annotated[str, Query(min_length=1, description="Full-text query over event metadata")],
    event_filter: EventFilterDep,
    service: SearchServiceDep,
    limit: Annotated[int, Query(ge=1, le=MAX_SEARCH_RESULTS)] = 20,
) -> SearchResultOut:
    """Full-text search across event metadata (Elasticsearch), best match first.

    Supports simple query syntax: `"exact phrase"`, `-exclude`, `prefix*`, `a | b`.
    """
    return SearchResultOut.from_domain(await service.search(q, event_filter, limit))


@router.get("/stats")
async def event_stats(
    event_filter: EventFilterDep,
    service: QueryServiceDep,
    bucket: TimeBucket = TimeBucket.DAY,
) -> EventStatsOut:
    """Event counts grouped by event type and time bucket (MongoDB aggregation).

    Missing bounds default to a recent window (24 hours, 30 days or 12 weeks).
    """
    return EventStatsOut.from_domain(await service.stats(event_filter, bucket))
