"""Liveness and readiness endpoints for orchestrators and load balancers."""

from fastapi import APIRouter, Response, status
from pydantic import BaseModel

from event_platform.api.dependencies import ReadinessServiceDep
from event_platform.application.health import HealthStatus

router = APIRouter(prefix="/health", tags=["health"])


class ComponentHealthOut(BaseModel):
    healthy: bool
    detail: str | None = None


class ReadinessOut(BaseModel):
    status: HealthStatus
    components: dict[str, ComponentHealthOut]


@router.get("")
async def liveness() -> dict[str, str]:
    """The process is up. Deliberately checks nothing else."""
    return {"status": "ok"}


@router.get("/ready", responses={503: {"description": "Instance cannot accept events"}})
async def readiness(service: ReadinessServiceDep, response: Response) -> ReadinessOut:
    """Per-dependency health. 503 only when this instance cannot ingest events."""
    report = await service.report()
    if not report.ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessOut(
        status=report.status,
        components={
            c.name: ComponentHealthOut(healthy=c.healthy, detail=c.detail)
            for c in report.components
        },
    )
