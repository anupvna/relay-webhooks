"""A tiny in-memory, per-IP rate limiter for the public demo routes.

Sliding window: remember each IP's request times for this route and reject
once there are `limit` of them inside the last `window` seconds.
In-memory is fine because we run one process; with several servers you'd keep
these counters in Redis or Postgres instead.
"""
import time
from collections import defaultdict, deque

from fastapi import HTTPException, Request


def client_ip(request: Request) -> str:
    # Behind a host's proxy, the real client IP is the first X-Forwarded-For entry.
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


class RateLimit:
    def __init__(self, limit: int, window: float = 60.0):
        self.limit = limit
        self.window = window
        self.hits: dict[str, deque] = defaultdict(deque)

    def __call__(self, request: Request) -> None:
        now = time.monotonic()
        hits = self.hits[client_ip(request)]
        while hits and hits[0] <= now - self.window:
            hits.popleft()
        if len(hits) >= self.limit:
            retry_after = int(self.window - (now - hits[0])) + 1
            raise HTTPException(
                429, "Too many requests, slow down",
                headers={"Retry-After": str(retry_after)},
            )
        hits.append(now)
