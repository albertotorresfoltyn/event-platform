"""Rate limiting contract shared by the HTTP middleware and its adapters."""

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    allowed: bool
    limit: int
    remaining: int
    reset_after_seconds: int


class RateLimiter(Protocol):
    async def hit(self, client_key: str) -> RateLimitDecision:
        """Count one request for ``client_key``.

        Raises ``DependencyUnavailableError`` when the backing store is unreachable.
        """
