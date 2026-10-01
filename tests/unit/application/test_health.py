import asyncio

from event_platform.application.health import ComponentHealth, HealthStatus, ReadinessService


async def healthy() -> None:
    return None


async def broken() -> None:
    raise ConnectionError("down")


async def hanging() -> None:
    await asyncio.sleep(10)


def make_service(**checks: object) -> ReadinessService:
    return ReadinessService(
        checks,  # type: ignore[arg-type]
        critical=frozenset({"queue"}),
        timeout_seconds=0.05,
    )


async def test_all_healthy_is_ok() -> None:
    report = await make_service(queue=healthy, mongodb=healthy).report()

    assert report.status is HealthStatus.OK
    assert report.ready


async def test_non_critical_failure_is_degraded_but_ready() -> None:
    report = await make_service(queue=healthy, mongodb=broken).report()

    assert report.status is HealthStatus.DEGRADED
    assert report.ready
    assert ComponentHealth("mongodb", healthy=False, detail="ConnectionError") in report.components


async def test_critical_failure_is_unavailable() -> None:
    report = await make_service(queue=broken, mongodb=healthy).report()

    assert report.status is HealthStatus.UNAVAILABLE
    assert not report.ready


async def test_slow_check_times_out() -> None:
    report = await make_service(queue=healthy, redis=hanging).report()

    assert ComponentHealth("redis", healthy=False, detail="timed out") in report.components
