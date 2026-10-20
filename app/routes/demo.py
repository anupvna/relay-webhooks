"""Public demo routes. Everything here only ever touches the built-in demo endpoint."""
import asyncio
import random
from collections import Counter

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.auth import require_admin
from app.config import settings
from app.ratelimit import RateLimit
from app.signing import verify_signature

router = APIRouter(prefix="/demo")

TIMEOUT_SHARE = 0.2  # of failures, how many hang past the timeout instead of returning 500


class DemoState:
    """In-memory state of the flaky receiver."""

    def __init__(self, failure_rate: float):
        self.failure_rate = failure_rate
        # Successful (2xx) receipts per event id. Any count above 1 is a duplicate delivery.
        self.receipts: Counter = Counter()


class FailureRate(BaseModel):
    rate: float = Field(ge=0.0, le=1.0)


class SendBatch(BaseModel):
    count: int = Field(20, ge=1, le=50)


@router.post("/receiver")
async def flaky_receiver(request: Request):
    """Acts like a real customer's webhook endpoint: verifies, then sometimes fails."""
    state: DemoState = request.app.state.demo
    body = await request.body()  # raw bytes: the signature covers these exactly
    h = request.headers
    if not verify_signature(
        request.app.state.demo_secret,
        h.get("X-Webhook-Id", ""),
        h.get("X-Webhook-Timestamp", ""),
        h.get("X-Webhook-Signature", ""),
        body,
    ):
        return JSONResponse({"error": "invalid signature"}, status_code=401)

    if random.random() < state.failure_rate:
        if random.random() < TIMEOUT_SHARE:
            # Hang longer than the sender's timeout, then fail anyway.
            await asyncio.sleep(settings.delivery_timeout + 1)
        return JSONResponse({"error": "simulated failure"}, status_code=500)

    state.receipts[h["X-Webhook-Id"]] += 1
    return {"received": True}


@router.get("/failure-rate")
async def get_failure_rate(request: Request):
    return {"rate": request.app.state.demo.failure_rate}


@router.put("/failure-rate", dependencies=[Depends(RateLimit(30))])
async def set_failure_rate(body: FailureRate, request: Request):
    request.app.state.demo.failure_rate = body.rate
    return {"rate": body.rate}


@router.post("/send-batch", status_code=201, dependencies=[Depends(RateLimit(6))])
async def send_batch(request: Request, body: SendBatch | None = None):
    """Queue N events for the demo endpoint in a single INSERT."""
    count = (body or SendBatch()).count
    ids = await request.app.state.pool.fetch(
        """INSERT INTO events (endpoint_id, type, payload)
           SELECT $1, 'demo.ping', jsonb_build_object('n', n, 'sent_at', now())
           FROM generate_series(1, $2) AS n
           RETURNING id""",
        request.app.state.demo_endpoint_id,
        count,
    )
    return {"created": len(ids)}


@router.get("/receiver-stats")
async def receiver_stats(request: Request):
    receipts = request.app.state.demo.receipts
    return {
        "unique_events_received": len(receipts),
        "duplicate_deliveries": sum(c - 1 for c in receipts.values() if c > 1),
    }


@router.post("/receiver-stats/reset", dependencies=[Depends(require_admin)])
async def reset_receiver_stats(request: Request):
    request.app.state.demo.receipts.clear()
    return {"reset": True}
