"""Realtime stats use case: a cheap summary served cache-aside from Redis.

Strategy (see ARCHITECTURE.md for the full discussion):

* **Cache-aside with TTL expiry, no write-path invalidation.** Every ingested event
  changes the counts, so invalidating on write would make the hit rate ~0 at any
  meaningful volume. Instead staleness is bounded by the TTL.
* **Single-flight on miss.** Concurrent requests that miss in the same process wait
  for one recomputation instead of all hitting MongoDB (cache-stampede guard).
* **Fail open.** The cache is an optimisation: if Redis is down, the summary is
  computed from MongoDB and the request still succeeds.
"""

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from event_platform.application.errors import DependencyUnavailableError
from event_platform.application.ports import EventReader, RealtimeStatsCache
from event_platform.application.queries import EventFilter, RealtimeStats

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class RealtimeStatsResult:
    stats: RealtimeStats
    cache_hit: bool


class RealtimeStatsService:
    def __init__(
        self,
        reader: EventReader,
        cache: RealtimeStatsCache,
        *,
        window: timedelta,
        ttl_seconds: int,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._reader = reader
        self._cache = cache
        self._window = window
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._recompute_lock = asyncio.Lock()

    @property
    def ttl_seconds(self) -> int:
        return self._ttl_seconds

    async def summary(self) -> RealtimeStatsResult:
        if cached := await self._cached():
            return RealtimeStatsResult(stats=cached, cache_hit=True)
        async with self._recompute_lock:
            # Another request may have refreshed the cache while we waited for the lock.
            if cached := await self._cached():
                return RealtimeStatsResult(stats=cached, cache_hit=True)
            stats = await self._compute()
            await self._store(stats)
            return RealtimeStatsResult(stats=stats, cache_hit=False)

    async def _compute(self) -> RealtimeStats:
        now = self._clock()
        window_start = now - self._window
        counts = await self._reader.count_by_type(EventFilter(start=window_start, end=now))
        return RealtimeStats(
            generated_at=now,
            window_start=window_start,
            window_end=now,
            total=sum(counts.values()),
            counts_by_type=counts,
        )

    async def _cached(self) -> RealtimeStats | None:
        try:
            return await self._cache.get()
        except DependencyUnavailableError:
            logger.warning("Realtime stats cache unavailable, computing from MongoDB")
            return None

    async def _store(self, stats: RealtimeStats) -> None:
        try:
            await self._cache.set(stats, self._ttl_seconds)
        except DependencyUnavailableError:
            logger.warning("Realtime stats cache unavailable, result not cached")
