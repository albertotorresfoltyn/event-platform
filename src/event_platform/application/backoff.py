"""Retry delay policy for failed event processing."""

import random
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExponentialBackoff:
    """Exponential backoff with "equal jitter".

    The ceiling doubles on each attempt (capped at ``max_seconds``) and the delay is
    drawn uniformly from ``[ceiling / 2, ceiling]``: jitter spreads retries out so a
    recovering MongoDB is not hit by a synchronised thundering herd, while the
    lower bound guarantees retries never fire back-to-back.
    """

    base_seconds: float
    max_seconds: float
    random: Callable[[], float] = random.random

    def delay_for(self, attempt: int) -> float:
        ceiling = min(self.max_seconds, self.base_seconds * 2.0 ** max(attempt - 1, 0))
        return ceiling / 2 + self.random() * ceiling / 2
