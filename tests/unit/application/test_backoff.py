import pytest

from event_platform.application.backoff import ExponentialBackoff


@pytest.mark.parametrize(
    ("attempt", "expected_ceiling"),
    [(1, 1.0), (2, 2.0), (3, 4.0), (6, 32.0), (7, 60.0), (50, 60.0)],
)
def test_ceiling_doubles_per_attempt_and_is_capped(attempt: int, expected_ceiling: float) -> None:
    upper = ExponentialBackoff(base_seconds=1, max_seconds=60, random=lambda: 1.0)
    lower = ExponentialBackoff(base_seconds=1, max_seconds=60, random=lambda: 0.0)

    assert upper.delay_for(attempt) == expected_ceiling
    assert lower.delay_for(attempt) == expected_ceiling / 2


def test_attempt_zero_is_treated_as_first_attempt() -> None:
    backoff = ExponentialBackoff(base_seconds=2, max_seconds=60, random=lambda: 1.0)

    assert backoff.delay_for(0) == 2.0
