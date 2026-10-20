"""Integration tests: real Postgres, real worker code, scripted receivers."""
import asyncio
import random

import httpx

from app.worker import claim_events, record_result, run_once, send_webhook
from tests.conftest import FakeReceiver, drain, make_endpoint, make_events


async def attempts_for(pool, event_id):
    return await pool.fetch(
        "SELECT attempt_no, status_code, error FROM delivery_attempts WHERE event_id = $1 ORDER BY attempt_no",
        event_id,
    )


async def test_pending_event_is_delivered_to_healthy_receiver(pool, cfg):
    receiver = FakeReceiver()
    [event_id] = await make_events(pool, await make_endpoint(pool))

    async with receiver.client() as client:
        await drain(pool, client, cfg)

    event = await pool.fetchrow("SELECT * FROM events WHERE id = $1", event_id)
    assert event["status"] == "delivered"
    assert event["attempts"] == 1
    assert event["delivered_at"] is not None
    assert event["locked_until"] is None
    assert [a["status_code"] for a in await attempts_for(pool, event_id)] == [200]
    assert receiver.bad_signatures == 0  # every request was correctly signed


async def test_receiver_fails_twice_then_succeeds(pool, cfg):
    receiver = FakeReceiver(responses=[500, 503, 200])
    [event_id] = await make_events(pool, await make_endpoint(pool))

    async with receiver.client() as client:
        await drain(pool, client, cfg)

    event = await pool.fetchrow("SELECT status, attempts FROM events WHERE id = $1", event_id)
    attempts = await attempts_for(pool, event_id)
    assert event["status"] == "delivered"
    assert event["attempts"] == 3
    assert [a["attempt_no"] for a in attempts] == [1, 2, 3]
    assert [a["status_code"] for a in attempts] == [500, 503, 200]
    assert attempts[0]["error"] == "HTTP 500"


async def test_timeouts_and_connection_errors_count_as_failures(pool, cfg):
    receiver = FakeReceiver(
        responses=[httpx.ReadTimeout("slow"), httpx.ConnectError("refused"), 200]
    )
    [event_id] = await make_events(pool, await make_endpoint(pool))

    async with receiver.client() as client:
        await drain(pool, client, cfg)

    attempts = await attempts_for(pool, event_id)
    assert [a["status_code"] for a in attempts] == [None, None, 200]
    assert attempts[0]["error"].startswith("Timed out")
    assert attempts[1]["error"].startswith("ConnectError")


async def test_always_failing_receiver_dead_letters_after_max_attempts_then_replay(pool, cfg):
    receiver = FakeReceiver(default=500)
    [event_id] = await make_events(pool, await make_endpoint(pool))

    async with receiver.client() as client:
        await drain(pool, client, cfg)

        event = await pool.fetchrow("SELECT status, attempts FROM events WHERE id = $1", event_id)
        assert event["status"] == "dead"
        assert event["attempts"] == cfg.max_attempts
        assert len(await attempts_for(pool, event_id)) == cfg.max_attempts

        # Dead events stay dead: the worker never picks them up again.
        assert await run_once(pool, client, cfg) == 0

        # Receiver recovers; replay (same statement the API route runs) resets the budget.
        receiver.default = 200
        await pool.execute(
            """UPDATE events SET status = 'pending', attempts = 0, next_attempt_at = now(),
                   locked_until = NULL WHERE id = $1 AND status = 'dead'""",
            event_id,
        )
        await drain(pool, client, cfg)

    event = await pool.fetchrow("SELECT status, attempts FROM events WHERE id = $1", event_id)
    assert event["status"] == "delivered"
    assert event["attempts"] == 1  # fresh budget after replay
    assert len(await attempts_for(pool, event_id)) == cfg.max_attempts + 1  # history is kept


async def test_concurrent_workers_never_double_claim(pool, cfg):
    """8 workers race over the same 300 events. Every event must be delivered
    exactly once: no event claimed by two workers, none skipped."""

    received = []

    async def handler(request):
        received.append(request.headers["X-Webhook-Id"])
        await asyncio.sleep(random.uniform(0, 0.01))  # random latency interleaves the workers
        return httpx.Response(200)

    endpoint_id = await make_endpoint(pool)
    event_ids = await make_events(pool, endpoint_id, n=300)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:

        async def worker():
            idle = 0
            while idle < 5:
                idle = idle + 1 if await run_once(pool, client, cfg) == 0 else 0

        await asyncio.gather(*(worker() for _ in range(8)))

    assert len(received) == 300                          # nothing sent twice
    assert set(received) == {str(i) for i in event_ids}  # nothing missed
    delivered = await pool.fetchval("SELECT count(*) FROM events WHERE status = 'delivered'")
    assert delivered == 300
    assert await pool.fetchval("SELECT count(*) FROM delivery_attempts") == 300


async def test_expired_lease_is_recovered_after_worker_crash(pool, cfg):
    """A worker claimed this event and crashed: it's stuck in 'delivering' with an
    expired lease. Another worker must pick it up and deliver it."""
    receiver = FakeReceiver()
    [stuck] = await make_events(pool, await make_endpoint(pool))
    await pool.execute(
        """UPDATE events SET status = 'delivering', locked_until = now() - interval '1 second'
           WHERE id = $1""",
        stuck,
    )

    async with receiver.client() as client:
        assert await run_once(pool, client, cfg) == 1

    event = await pool.fetchrow("SELECT status, attempts FROM events WHERE id = $1", stuck)
    assert event["status"] == "delivered"
    assert event["attempts"] == 1


async def test_active_lease_is_not_stolen(pool, cfg):
    """An event mid-delivery by a live worker (lease still valid) is left alone."""
    [busy] = await make_events(pool, await make_endpoint(pool))
    await pool.execute(
        "UPDATE events SET status = 'delivering', locked_until = now() + interval '30 seconds' WHERE id = $1",
        busy,
    )
    async with FakeReceiver().client() as client:
        assert await run_once(pool, client, cfg) == 0


async def test_stale_worker_cannot_overwrite_after_losing_lease(pool, cfg):
    """Fencing: worker A's lease expires and worker B re-claims the event.
    When A finally reports, its write must be ignored."""
    [event_id] = await make_events(pool, await make_endpoint(pool))
    [claim_a] = await claim_events(pool, cfg)

    # A is slow; its lease expires and B takes over and delivers successfully.
    await pool.execute(
        "UPDATE events SET locked_until = now() - interval '1 second' WHERE id = $1", event_id
    )
    async with FakeReceiver().client() as client:
        assert await run_once(pool, client, cfg) == 1

        # A comes back with a failure result. It must not undo B's delivery.
        result_a = await send_webhook(
            httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500))),
            claim_a, cfg.timeout,
        )
        await record_result(pool, claim_a, result_a, cfg)

    event = await pool.fetchrow("SELECT status FROM events WHERE id = $1", event_id)
    assert event["status"] == "delivered"
