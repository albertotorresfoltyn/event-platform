from datetime import UTC, datetime, timedelta

from event_platform.application.queries import RealtimeStats
from event_platform.infrastructure.redis.realtime_stats_cache import (
    SCHEMA_VERSION,
    RedisRealtimeStatsCache,
    deserialize,
    serialize,
)

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def test_stats_round_trip_through_serialization() -> None:
    stats = RealtimeStats(
        generated_at=NOW,
        window_start=NOW - timedelta(hours=1),
        window_end=NOW,
        total=4,
        counts_by_type={"click": 1, "pageview": 3},
    )

    assert deserialize(serialize(stats)) == stats
    assert deserialize(serialize(stats).encode()) == stats


def test_key_is_namespaced_versioned_and_window_specific() -> None:
    cache = RedisRealtimeStatsCache(client=None, key_prefix="app", window_seconds=3600)  # type: ignore[arg-type]

    assert cache.key == f"app:stats:realtime:{SCHEMA_VERSION}:3600s"
