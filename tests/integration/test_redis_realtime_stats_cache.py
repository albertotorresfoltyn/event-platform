from datetime import UTC, datetime, timedelta

import pytest
from redis.asyncio import Redis

from event_platform.application.queries import RealtimeStats
from event_platform.infrastructure.redis.realtime_stats_cache import RedisRealtimeStatsCache

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
STATS = RealtimeStats(
    generated_at=NOW,
    window_start=NOW - timedelta(hours=1),
    window_end=NOW,
    total=3,
    counts_by_type={"click": 3},
)


@pytest.fixture
def cache(redis_client: Redis, redis_key_prefix: str) -> RedisRealtimeStatsCache:
    return RedisRealtimeStatsCache(redis_client, key_prefix=redis_key_prefix, window_seconds=3600)


async def test_empty_cache_returns_none(cache: RedisRealtimeStatsCache) -> None:
    assert await cache.get() is None


async def test_stored_stats_are_returned_with_the_configured_ttl(
    cache: RedisRealtimeStatsCache, redis_client: Redis
) -> None:
    await cache.set(STATS, ttl_seconds=30)

    assert await cache.get() == STATS
    assert 0 < await redis_client.ttl(cache.key) <= 30
