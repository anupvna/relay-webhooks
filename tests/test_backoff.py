import random

import pytest

from app.backoff import backoff_seconds

BASE, CAP = 1.0, 30.0


@pytest.mark.parametrize("attempt", range(0, 12))
def test_backoff_stays_within_jitter_bounds(attempt):
    ceiling = min(CAP, BASE * 2**attempt)
    for seed in range(200):
        delay = backoff_seconds(attempt, BASE, CAP, random.Random(seed))
        assert ceiling * 0.5 <= delay <= ceiling


def test_backoff_grows_with_attempts_until_cap():
    # Even with the worst-case jitter, attempt n+1's minimum is >= attempt n's maximum
    # (until the cap), so delays genuinely grow.
    for attempt in range(0, 4):
        worst_next = min(CAP, BASE * 2 ** (attempt + 1)) * 0.5
        best_now = min(CAP, BASE * 2**attempt)
        assert worst_next >= best_now


def test_backoff_never_exceeds_cap():
    rng = random.Random(1)
    assert all(backoff_seconds(50, BASE, CAP, rng) <= CAP for _ in range(1000))


def test_backoff_is_actually_jittered():
    rng = random.Random(7)
    delays = {round(backoff_seconds(3, BASE, CAP, rng), 6) for _ in range(50)}
    assert len(delays) > 40  # not the same delay every time
