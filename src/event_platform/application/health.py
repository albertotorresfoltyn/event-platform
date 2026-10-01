"""Readiness reporting across the platform's dependencies.

Readiness fails (HTTP 503) only when a *critical* component is down: the ingestion
queue being full. A shared dependency outage (MongoDB, Elasticsearch, Redis) is
reported as ``degraded`` but keeps the instance in rotation: every instance shares
those dependencies, so pulling instances out would turn a partial outage (reads
fail, ingestion still buffers) into a total one.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum

DependencyCheck = Callable[[], Awaitable[None]]


class HealthStatus(StrEnum):
    OK = "ok"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class ComponentHealth:
    name: str
    healthy: bool
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class ReadinessReport:
    status: HealthStatus
    components: list[ComponentHealth]

    @property
    def ready(self) -> bool:
        return self.status is not HealthStatus.UNAVAILABLE


class ReadinessService:
    def __init__(
        self,
        checks: dict[str, DependencyCheck],
        *,
        critical: frozenset[str],
        timeout_seconds: float,
    ) -> None:
        self._checks = checks
        self._critical = critical
        self._timeout_seconds = timeout_seconds

    async def report(self) -> ReadinessReport:
        components = list(
            await asyncio.gather(*(self._run(name, check) for name, check in self._checks.items()))
        )
        unhealthy = {c.name for c in components if not c.healthy}
        if unhealthy & self._critical:
            status = HealthStatus.UNAVAILABLE
        elif unhealthy:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.OK
        return ReadinessReport(status=status, components=components)

    async def _run(self, name: str, check: DependencyCheck) -> ComponentHealth:
        try:
            async with asyncio.timeout(self._timeout_seconds):
                await check()
        except TimeoutError:
            return ComponentHealth(name, healthy=False, detail="timed out")
        except Exception as exc:
            return ComponentHealth(name, healthy=False, detail=type(exc).__name__)
        return ComponentHealth(name, healthy=True)
