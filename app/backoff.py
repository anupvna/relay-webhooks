"""Exponential backoff with jitter.

Exponential: each failure waits roughly twice as long, giving a struggling
receiver room to recover instead of hammering it.

Jitter: if a receiver goes down, hundreds of events fail at the same moment.
Without randomness they'd all retry at the same moment too (a "thundering
herd") and knock it over again. Multiplying by a random 0.5-1.0 spreads them out.
"""
import random


def backoff_seconds(
    attempt: int, base: float, cap: float, rng: random.Random | None = None
) -> float:
    rng = rng or random
    ceiling = min(cap, base * 2**attempt)
    return ceiling * rng.uniform(0.5, 1.0)
