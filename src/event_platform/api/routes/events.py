"""Event endpoints."""

from fastapi import APIRouter, status

from event_platform.api.dependencies import IngestionServiceDep
from event_platform.api.schemas import EventAccepted, EventIn

router = APIRouter(prefix="/events", tags=["events"])


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
