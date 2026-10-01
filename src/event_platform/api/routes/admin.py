"""Operational endpoints. Disabled unless ADMIN_API_KEY is configured."""

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from event_platform.api.dependencies import DeadLetterQueueDep, require_admin
from event_platform.api.schemas import EventOut

router = APIRouter(
    prefix="/admin", tags=["admin"], dependencies=[Depends(require_admin)], include_in_schema=True
)


class DeadLettersOut(BaseModel):
    count: int
    items: list[EventOut]


class RedriveOut(BaseModel):
    redriven: int


@router.get("/dead-letters")
async def list_dead_letters(dead_letters: DeadLetterQueueDep) -> DeadLettersOut:
    """Events that exhausted their retries."""
    events = await dead_letters.list_dead_letters()
    return DeadLettersOut(count=len(events), items=[EventOut.from_domain(e) for e in events])


@router.post("/dead-letters/redrive")
async def redrive_dead_letters(dead_letters: DeadLetterQueueDep) -> RedriveOut:
    """Re-enqueue dead letters once the underlying failure has been fixed."""
    return RedriveOut(redriven=await dead_letters.redrive_dead_letters())
