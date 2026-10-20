"""Shared test fixtures.

Integration tests run against a real Postgres database named relay_test, kept
separate from your dev database. Set TEST_DATABASE_URL to point elsewhere
(CI does this).

Receivers are faked with httpx.MockTransport: the worker sends real signed
requests, but they're answered by a Python function instead of the network,
so tests are fast and fully controlled.
"""
import os
from urllib.parse import urlparse, urlunparse

import asyncpg
import httpx
import pytest

from app import db
from app.signing import verify_signature
from app.worker import WorkerConfig, run_once

TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL", "postgresql://relay:relay@localhost:5433/relay_test"
)
SECRET = "test-secret"


async def _ensure_database_exists(url: str) -> None:
    parsed = urlparse(url)
    name = parsed.path.lstrip("/")
    admin_url = urlunparse(parsed._replace(path="/postgres"))
    conn = await asyncpg.connect(admin_url)
    try:
        if not await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", name):
            await conn.execute(f'CREATE DATABASE "{name}"')
    finally:
        await conn.close()


@pytest.fixture
async def pool():
    await _ensure_database_exists(TEST_DATABASE_URL)
    pool = await db.create_pool(TEST_DATABASE_URL)
    await db.run_migrations(pool)
    await pool.execute("TRUNCATE endpoints, events, delivery_attempts CASCADE")
    yield pool
    await pool.close()


@pytest.fixture
def cfg():
    # Same logic as production, but retries every few milliseconds so tests are quick.
    return WorkerConfig(
        batch_size=10, lease_seconds=30, timeout=1.0,
        max_attempts=6, backoff_base=0.001, backoff_cap=0.005, idle_sleep=0.01,
    )


class FakeReceiver:
    """A receiver we script per test. It verifies signatures like a real one would
    and records every request so tests can assert on them."""

    def __init__(self, responses=None, default=200):
        self.responses = list(responses or [])  # status codes (or exceptions) to return, in order
        self.default = default
        self.requests = []          # every request received
        self.successes = {}         # event id -> number of 2xx responses sent
        self.bad_signatures = 0

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        body = request.content
        h = request.headers
        if not verify_signature(
            SECRET, h["X-Webhook-Id"], h["X-Webhook-Timestamp"], h["X-Webhook-Signature"], body
        ):
            self.bad_signatures += 1
            return httpx.Response(401)
        self.requests.append(request)
        outcome = self.responses.pop(0) if self.responses else self.default
        if isinstance(outcome, Exception):
            raise outcome
        if 200 <= outcome < 300:
            event_id = h["X-Webhook-Id"]
            self.successes[event_id] = self.successes.get(event_id, 0) + 1
        return httpx.Response(outcome)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self))


async def make_endpoint(pool, url="https://receiver.test/hook", secret=SECRET):
    return await pool.fetchval(
        "INSERT INTO endpoints (url, secret) VALUES ($1, $2) RETURNING id", url, secret
    )


async def make_events(pool, endpoint_id, n=1):
    rows = await pool.fetch(
        """INSERT INTO events (endpoint_id, type, payload)
           SELECT $1, 'test.event', jsonb_build_object('n', n)
           FROM generate_series(1, $2) AS n RETURNING id""",
        endpoint_id, n,
    )
    return [r["id"] for r in rows]


async def drain(pool, client, cfg, max_rounds=500):
    """Run the worker until no event is pending or delivering."""
    import asyncio

    for _ in range(max_rounds):
        await run_once(pool, client, cfg)
        open_count = await pool.fetchval(
            "SELECT count(*) FROM events WHERE status IN ('pending', 'delivering')"
        )
        if open_count == 0:
            return
        await asyncio.sleep(0.005)  # let backoff timers elapse
    raise AssertionError("events never settled")
