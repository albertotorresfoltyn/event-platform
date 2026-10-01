"""Redis implementation of the RealtimeStatsCache port."""

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from event_platform.application.errors import DependencyUnavailableError
from event_platform.application.queries import RealtimeStats

# Bump the version whenever the serialized shape changes, so old entries are ignored
# instead of failing to deserialize during a rolling deploy.
SCHEMA_VERSION = "v1"


@asynccontextmanager
async def _translate_errors() -> AsyncIterator[None]:
    try:
        yield
    except (RedisConnectionError, RedisTimeoutError) as exc:
        raise DependencyUnavailableError("Redis") from exc


def serialize(stats: RealtimeStats) -> str:
    return json.dumps(
        {
            "generated_at": stats.generated_at.isoformat(),
            "window_start": stats.window_start.isoformat(),
            "window_end": stats.window_end.isoformat(),
            "total": stats.total,
            "counts_by_type": stats.counts_by_type,
        }
    )


def deserialize(raw: str | bytes) -> RealtimeStats:
    data: dict[str, Any] = json.loads(raw)
    return RealtimeStats(
        generated_at=datetime.fromisoformat(data["generated_at"]),
        window_start=datetime.fromisoformat(data["window_start"]),
        window_end=datetime.fromisoformat(data["window_end"]),
        total=data["total"],
        counts_by_type=data["counts_by_type"],
    )


class RedisRealtimeStatsCache:
    def __init__(self, client: "Redis", key_prefix: str, window_seconds: int) -> None:
        # The window is part of the key: changing it must not serve a differently-sized summary.
        self._key = f"{key_prefix}:stats:realtime:{SCHEMA_VERSION}:{window_seconds}s"
        self._client = client

    @property
    def key(self) -> str:
        return self._key

    async def get(self) -> RealtimeStats | None:
        async with _translate_errors():
            raw = await self._client.get(self._key)
        return deserialize(raw) if raw is not None else None

    async def set(self, stats: RealtimeStats, ttl_seconds: int) -> None:
        async with _translate_errors():
            await self._client.set(self._key, serialize(stats), ex=ttl_seconds)
