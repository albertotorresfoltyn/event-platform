import asyncio
from datetime import UTC, datetime, timedelta

from event_platform.application.queries import EventFilter, RealtimeStats
from event_platform.application.realtime_stats import RealtimeStatsService
from tests.fakes import FakeRealtimeStatsCache, StubEventReader

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
WINDOW = timedelta(hours=1)
TTL = 10


def make_service(reader: StubEventReader, cache: FakeRealtimeStatsCache) -> RealtimeStatsService:
    return RealtimeStatsService(reader, cache, window=WINDOW, ttl_seconds=TTL, clock=lambda: NOW)


async def test_miss_computes_from_the_reader_and_caches_with_ttl() -> None:
    reader = StubEventReader(counts_by_type={"click": 3, "pageview": 7})
    cache = FakeRealtimeStatsCache()

    result = await make_service(reader, cache).summary()

    assert result.cache_hit is False
    assert result.stats == RealtimeStats(
        generated_at=NOW,
        window_start=NOW - WINDOW,
        window_end=NOW,
        total=10,
        counts_by_type={"click": 3, "pageview": 7},
    )
    assert reader.count_by_type_calls == [EventFilter(start=NOW - WINDOW, end=NOW)]
    assert (cache.stored, cache.ttl_seconds) == (result.stats, TTL)


async def test_hit_is_served_without_touching_the_reader() -> None:
    reader, cache = StubEventReader(counts_by_type={"click": 1}), FakeRealtimeStatsCache()
    service = make_service(reader, cache)
    await service.summary()

    result = await service.summary()

    assert result.cache_hit is True
    assert len(reader.count_by_type_calls) == 1


async def test_concurrent_misses_trigger_a_single_recomputation() -> None:
    reader = StubEventReader(counts_by_type={"click": 1}, delay_seconds=0.05)
    service = make_service(reader, FakeRealtimeStatsCache())

    results = await asyncio.gather(*(service.summary() for _ in range(10)))

    assert len(reader.count_by_type_calls) == 1
    assert sum(not r.cache_hit for r in results) == 1


async def test_unavailable_cache_falls_back_to_the_reader() -> None:
    reader = StubEventReader(counts_by_type={"click": 2})
    service = make_service(reader, FakeRealtimeStatsCache(available=False))

    first, second = await service.summary(), await service.summary()

    assert first.stats.total == second.stats.total == 2
    assert not first.cache_hit
    assert not second.cache_hit
    assert len(reader.count_by_type_calls) == 2
