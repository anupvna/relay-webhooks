"""The delivery worker: claim -> deliver -> record -> decide what happens next.

Several copies of `worker_loop` run as asyncio tasks inside the API process.
They coordinate only through Postgres, so the same code works unchanged if you
later run workers in separate processes or on separate machines.
"""
import asyncio
import json
import logging
import time
from dataclasses import dataclass

import asyncpg
import httpx

from app.backoff import backoff_seconds
from app.signing import sign_headers

log = logging.getLogger("relay.worker")


@dataclass
class WorkerConfig:
    batch_size: int = 10
    lease_seconds: int = 30
    timeout: float = 5.0
    max_attempts: int = 6
    backoff_base: float = 1.0
    backoff_cap: float = 30.0
    idle_sleep: float = 0.5


# Claim up to $1 due events in ONE statement.
#  - Due = pending and its retry time has come, OR stuck in 'delivering' with an
#    expired lease (the worker holding it crashed).
#  - FOR UPDATE SKIP LOCKED: rows another worker is claiming right now are
#    skipped instead of waited on, so two workers never get the same event.
#  - The lease (locked_until) marks the event as ours for $2 seconds.
CLAIM_SQL = """
UPDATE events e
SET status = 'delivering',
    locked_until = now() + make_interval(secs => $2)
FROM endpoints ep
WHERE ep.id = e.endpoint_id
  AND e.id IN (
      SELECT id FROM events
      WHERE (status = 'pending' AND next_attempt_at <= now())
         OR (status = 'delivering' AND locked_until < now())
      ORDER BY next_attempt_at
      FOR UPDATE SKIP LOCKED
      LIMIT $1
  )
RETURNING e.id, e.type, e.payload, e.attempts, e.locked_until, ep.url, ep.secret
"""


async def claim_events(pool: asyncpg.Pool, cfg: WorkerConfig) -> list[asyncpg.Record]:
    return await pool.fetch(CLAIM_SQL, cfg.batch_size, cfg.lease_seconds)


async def send_webhook(client: httpx.AsyncClient, event, timeout: float) -> dict:
    """POST one signed webhook. Never raises: every failure becomes a result."""
    event_id = str(event["id"])
    # Serialize once and sign those exact bytes. Re-serializing could change
    # whitespace or key order and break the signature.
    body = json.dumps(
        {"id": event_id, "type": event["type"], "data": event["payload"]},
        separators=(",", ":"),
    ).encode()
    headers = sign_headers(event["secret"], event_id, body)

    started = time.perf_counter()
    status_code, error = None, None
    try:
        resp = await client.post(
            event["url"],
            content=body,
            headers={**headers, "Content-Type": "application/json"},
            timeout=timeout,
        )
        status_code = resp.status_code
        if not 200 <= status_code < 300:
            error = f"HTTP {status_code}"
    except httpx.TimeoutException:
        error = f"Timed out after {timeout:g}s"
    except httpx.HTTPError as exc:
        error = f"{type(exc).__name__}: {exc}"[:500]

    return {
        "ok": error is None,
        "status_code": status_code,
        "latency_ms": int((time.perf_counter() - started) * 1000),
        "error": error,
        "headers": headers,
    }


async def record_result(pool: asyncpg.Pool, event, result: dict, cfg: WorkerConfig) -> str:
    """Log the attempt and move the event to its next state, in one transaction."""
    attempts = event["attempts"] + 1  # counts every HTTP try, successful or not

    if result["ok"]:
        new_status, next_at_delay = "delivered", None
    elif attempts >= cfg.max_attempts:
        new_status, next_at_delay = "dead", None
    else:
        new_status = "pending"
        next_at_delay = backoff_seconds(attempts, cfg.backoff_base, cfg.backoff_cap)

    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            """INSERT INTO delivery_attempts
                   (event_id, attempt_no, status_code, latency_ms, error, request_headers)
               VALUES ($1, $2, $3, $4, $5, $6)""",
            event["id"], attempts, result["status_code"], result["latency_ms"],
            result["error"], result["headers"],
        )
        # `locked_until = $5` is a fencing check: we only update the event if we
        # still hold the lease we claimed. If our lease expired and another worker
        # took over, our stale write is ignored instead of clobbering theirs.
        updated = await conn.execute(
            """UPDATE events
               SET status = $2,
                   attempts = $3,
                   locked_until = NULL,
                   delivered_at = CASE WHEN $2 = 'delivered' THEN now() END,
                   next_attempt_at = CASE WHEN $4::float8 IS NULL THEN next_attempt_at
                                          ELSE now() + make_interval(secs => $4) END
               WHERE id = $1 AND status = 'delivering' AND locked_until = $5""",
            event["id"], new_status, attempts, next_at_delay, event["locked_until"],
        )
    if updated == "UPDATE 0":
        log.warning("Lost lease on event %s; another worker owns it now", event["id"])
    return new_status


async def process_event(pool, client, event, cfg: WorkerConfig) -> str:
    result = await send_webhook(client, event, cfg.timeout)
    return await record_result(pool, event, result, cfg)


async def run_once(pool, client, cfg: WorkerConfig) -> int:
    """Claim one batch and deliver it concurrently. Returns how many were claimed."""
    events = await claim_events(pool, cfg)
    if events:
        await asyncio.gather(*(process_event(pool, client, e, cfg) for e in events))
    return len(events)


async def worker_loop(name: str, pool, client, cfg: WorkerConfig, stop: asyncio.Event):
    log.info("%s started", name)
    while not stop.is_set():
        try:
            claimed = await run_once(pool, client, cfg)
        except Exception:
            # A DB hiccup must not kill the worker; log it and try again shortly.
            log.exception("%s: iteration failed", name)
            claimed = 0
        if claimed == 0:
            try:
                await asyncio.wait_for(stop.wait(), timeout=cfg.idle_sleep)
            except asyncio.TimeoutError:
                pass
    log.info("%s stopped", name)


async def cleanup_loop(pool, demo_endpoint_id, retention_minutes: int, stop: asyncio.Event):
    """Delete old demo events every 5 minutes so the free database never fills up.
    Their delivery_attempts go too, via ON DELETE CASCADE."""
    while not stop.is_set():
        try:
            await pool.execute(
                """DELETE FROM events
                   WHERE endpoint_id = $1
                     AND created_at < now() - make_interval(mins => $2)""",
                demo_endpoint_id, retention_minutes,
            )
        except Exception:
            log.exception("cleanup failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=300)
        except asyncio.TimeoutError:
            pass
