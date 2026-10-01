"""Fixed-window rate limiter backed by Redis, shared by every API instance.

One counter per client per window: ``INCR`` + ``EXPIRE`` run in a single
MULTI/EXEC transaction, so the first request of a window both creates the counter
and schedules its cleanup. Fixed windows allow up to 2x the limit across a window
boundary; that is an accepted trade-off for O(1) memory and one round trip per
request (a sliding-window log/counter is the upgrade path, see ARCHITECTURE.md).
"""

import time
from collections.abc import Callable

from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from event_platform.application.errors import DependencyUnavailableError
from event_platform.application.rate_limiting import RateLimitDecision


class RedisFixedWindowRateLimiter:
    def __init__(
        self,
        client: Redis,
        *,
        key_prefix: str,
        limit: int,
        window_seconds: int,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._client = client
        self._key_prefix = key_prefix
        self._limit = limit
        self._window_seconds = window_seconds
        self._clock = clock

    async def hit(self, client_key: str) -> RateLimitDecision:
        now = self._clock()
        window = int(now // self._window_seconds)
        key = f"{self._key_prefix}:ratelimit:{client_key}:{window}"
        try:
            async with self._client.pipeline(transaction=True) as pipeline:
                pipeline.incr(key)
                pipeline.expire(key, self._window_seconds)
                count, _ = await pipeline.execute()
        except (RedisConnectionError, RedisTimeoutError) as exc:
            raise DependencyUnavailableError("Redis") from exc

        reset_after = int((window + 1) * self._window_seconds - now) or 1
        return RateLimitDecision(
            allowed=count <= self._limit,
            limit=self._limit,
            remaining=max(self._limit - count, 0),
            reset_after_seconds=reset_after,
        )
