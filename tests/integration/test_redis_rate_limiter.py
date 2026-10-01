import pytest
from redis.asyncio import Redis

from event_platform.infrastructure.redis.rate_limiter import RedisFixedWindowRateLimiter

pytestmark = pytest.mark.integration

WINDOW = 60


class FakeClock:
    def __init__(self) -> None:
        self.now = 1_800_000_000.0  # aligned to a 60s window boundary

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def limiter(
    redis_client: Redis, redis_key_prefix: str, clock: FakeClock
) -> RedisFixedWindowRateLimiter:
    return RedisFixedWindowRateLimiter(
        redis_client, key_prefix=redis_key_prefix, limit=2, window_seconds=WINDOW, clock=clock
    )


async def test_requests_beyond_the_limit_are_denied(limiter: RedisFixedWindowRateLimiter) -> None:
    decisions = [await limiter.hit("1.2.3.4") for _ in range(3)]

    assert [d.allowed for d in decisions] == [True, True, False]
    assert [d.remaining for d in decisions] == [1, 0, 0]
    assert decisions[0].reset_after_seconds == WINDOW


async def test_clients_are_counted_independently(limiter: RedisFixedWindowRateLimiter) -> None:
    await limiter.hit("1.1.1.1")
    await limiter.hit("1.1.1.1")

    assert (await limiter.hit("2.2.2.2")).allowed


async def test_counter_resets_in_the_next_window(
    limiter: RedisFixedWindowRateLimiter, clock: FakeClock
) -> None:
    for _ in range(3):
        await limiter.hit("1.2.3.4")

    clock.now += WINDOW

    assert (await limiter.hit("1.2.3.4")).allowed


async def test_counter_keys_expire_with_the_window(
    limiter: RedisFixedWindowRateLimiter, redis_client: Redis, redis_key_prefix: str
) -> None:
    await limiter.hit("1.2.3.4")

    [key] = [k async for k in redis_client.scan_iter(match=f"{redis_key_prefix}:ratelimit:*")]
    assert 0 < await redis_client.ttl(key) <= WINDOW
