from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.auth import is_admin, require_admin
from app.schemas import EventCreate

router = APIRouter()

EVENT_COLUMNS = """id, endpoint_id, type, payload, status, attempts,
                   next_attempt_at, locked_until, created_at, delivered_at"""


@router.post("/events", status_code=201, dependencies=[Depends(require_admin)])
async def create_event(body: EventCreate, request: Request):
    """Queue an event. It's just an INSERT; a worker picks it up within ~0.5s."""
    pool = request.app.state.pool
    if not await pool.fetchval("SELECT 1 FROM endpoints WHERE id = $1", body.endpoint_id):
        raise HTTPException(404, "Endpoint not found")
    row = await pool.fetchrow(
        f"""INSERT INTO events (endpoint_id, type, payload)
            VALUES ($1, $2, $3) RETURNING {EVENT_COLUMNS}""",
        body.endpoint_id,
        body.type,
        body.payload,
    )
    return dict(row)


@router.get("/events")
async def list_events(
    request: Request,
    status: Literal["pending", "delivering", "delivered", "dead"] | None = None,
    limit: int = Query(50, ge=1, le=200),
):
    """Most recent events. Without the admin key you only see demo events."""
    admin = is_admin(request)
    rows = await request.app.state.pool.fetch(
        f"""SELECT {EVENT_COLUMNS} FROM events
            WHERE ($1::text IS NULL OR status = $1)
              AND ($2 OR endpoint_id = $3)
            ORDER BY created_at DESC
            LIMIT $4""",
        status,
        admin,
        request.app.state.demo_endpoint_id,
        limit,
    )
    return [dict(r) for r in rows]


async def _get_visible_event(request: Request, event_id: UUID):
    """Fetch an event, hiding non-demo events from public callers (404, not 403,
    so outsiders can't even confirm the event exists)."""
    row = await request.app.state.pool.fetchrow(
        f"SELECT {EVENT_COLUMNS} FROM events WHERE id = $1", event_id
    )
    if row is None or (
        row["endpoint_id"] != request.app.state.demo_endpoint_id and not is_admin(request)
    ):
        raise HTTPException(404, "Event not found")
    return row


@router.get("/events/{event_id}")
async def get_event(event_id: UUID, request: Request):
    """One event plus every delivery attempt, oldest first."""
    event = await _get_visible_event(request, event_id)
    attempts = await request.app.state.pool.fetch(
        """SELECT attempt_no, status_code, latency_ms, error, request_headers, created_at
           FROM delivery_attempts WHERE event_id = $1 ORDER BY attempt_no""",
        event_id,
    )
    return {**dict(event), "delivery_attempts": [dict(a) for a in attempts]}


@router.post("/events/{event_id}/replay")
async def replay_event(event_id: UUID, request: Request):
    """Send a dead event back to the queue with a fresh retry budget."""
    await _get_visible_event(request, event_id)
    # The status check lives in the WHERE clause, so two concurrent replays
    # can't both succeed and a delivered event can never be resent by accident.
    row = await request.app.state.pool.fetchrow(
        f"""UPDATE events
            SET status = 'pending', attempts = 0, next_attempt_at = now(), locked_until = NULL
            WHERE id = $1 AND status = 'dead'
            RETURNING {EVENT_COLUMNS}""",
        event_id,
    )
    if row is None:
        raise HTTPException(409, "Only dead events can be replayed")
    return dict(row)


@router.post("/dlq/replay")
async def replay_dead_letter_queue(request: Request):
    """Replay every dead demo event in one statement."""
    count = await request.app.state.pool.fetchval(
        """WITH replayed AS (
               UPDATE events
               SET status = 'pending', attempts = 0, next_attempt_at = now(), locked_until = NULL
               WHERE status = 'dead' AND endpoint_id = $1
               RETURNING 1
           )
           SELECT count(*) FROM replayed""",
        request.app.state.demo_endpoint_id,
    )
    return {"replayed": count}
