"""Helpers for asserting on eventually-consistent, asynchronous outcomes."""

import asyncio
from collections.abc import Awaitable, Callable

DEFAULT_TIMEOUT_SECONDS = 5.0


async def wait_until(condition: Callable[[], Awaitable[bool]], interval: float = 0.05) -> None:
    """Poll ``condition`` until it holds; fails after ``DEFAULT_TIMEOUT_SECONDS``."""
    async with asyncio.timeout(DEFAULT_TIMEOUT_SECONDS):
        # Polling is intended: the condition reads external state (e.g. MongoDB).
        while not await condition():  # noqa: ASYNC110
            await asyncio.sleep(interval)
