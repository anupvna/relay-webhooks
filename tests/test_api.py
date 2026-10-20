"""API tests. The app runs in-process via ASGITransport; we attach the test DB pool
to app.state ourselves, so no background workers start during these tests."""
import time

import httpx
import pytest

from app.config import settings
from app.main import app
from app.routes.demo import DemoState
from app.signing import sign_headers
from tests.conftest import SECRET, make_endpoint, make_events

ADMIN = {"X-API-Key": "test-admin-key"}


@pytest.fixture
async def api(pool, monkeypatch):
    monkeypatch.setattr(settings, "admin_api_key", "test-admin-key")
    demo_id = await make_endpoint(pool, url="http://test/demo/receiver", secret=SECRET)
    await pool.execute("UPDATE endpoints SET is_demo = true WHERE id = $1", demo_id)
    app.state.pool = pool
    app.state.demo_endpoint_id = demo_id
    app.state.demo_secret = SECRET
    app.state.demo = DemoState(failure_rate=0.0)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        yield c


async def test_health(api):
    assert (await api.get("/health")).json() == {"status": "ok"}


async def test_registering_endpoints_requires_admin_key(api):
    body = {"url": "https://example.com/hook"}
    assert (await api.post("/endpoints", json=body)).status_code == 401
    assert (await api.post("/endpoints", json=body, headers={"X-API-Key": "wrong"})).status_code == 401
    resp = await api.post("/endpoints", json=body, headers=ADMIN)
    assert resp.status_code == 201
    assert len(resp.json()["secret"]) == 64


async def test_public_cannot_see_private_events(api, pool):
    private_ep = await make_endpoint(pool)
    [private_event] = await make_events(pool, private_ep)
    assert (await api.get(f"/events/{private_event}")).status_code == 404
    assert (await api.get(f"/events/{private_event}", headers=ADMIN)).status_code == 200
    assert (await api.get("/events")).json() == []


async def test_replay_only_works_on_dead_events(api, pool):
    [event_id] = await make_events(pool, app.state.demo_endpoint_id)
    assert (await api.post(f"/events/{event_id}/replay")).status_code == 409

    await pool.execute("UPDATE events SET status = 'dead', attempts = 6 WHERE id = $1", event_id)
    resp = await api.post(f"/events/{event_id}/replay")
    assert resp.status_code == 200
    assert resp.json()["status"] == "pending" and resp.json()["attempts"] == 0
    assert (await api.post(f"/events/{event_id}/replay")).status_code == 409  # can't double-replay


async def test_dlq_replay_and_stats(api, pool):
    ids = await make_events(pool, app.state.demo_endpoint_id, n=4)
    await pool.execute("UPDATE events SET status = 'dead', attempts = 6 WHERE id = ANY($1)", ids[:2])
    await pool.execute(
        "UPDATE events SET status = 'delivered', attempts = 2, delivered_at = now() WHERE id = ANY($1)",
        ids[2:],
    )
    stats = (await api.get("/stats")).json()
    assert stats["delivered_pct"] == 50.0 and stats["dead"] == 2 and stats["avg_attempts"] == 2.0

    assert (await api.post("/dlq/replay")).json() == {"replayed": 2}
    assert (await api.get("/stats")).json()["dead"] == 0


async def test_demo_receiver_rejects_bad_signatures(api):
    body = b'{"hello":"world"}'
    good = sign_headers(SECRET, "evt-1", body)
    assert (await api.post("/demo/receiver", content=body, headers=good)).status_code == 200

    forged = {**good, "X-Webhook-Signature": "0" * 64}
    assert (await api.post("/demo/receiver", content=body, headers=forged)).status_code == 401

    tampered = body.replace(b"world", b"earth")
    assert (await api.post("/demo/receiver", content=tampered, headers=good)).status_code == 401

    old = sign_headers(SECRET, "evt-2", body, now=int(time.time()) - 600)
    assert (await api.post("/demo/receiver", content=body, headers=old)).status_code == 401


async def test_send_batch_is_capped_and_rate_limited(api):
    # Too big: rejected by validation. It still uses up 1 of the 6 per-minute
    # slots, so hammering the API with junk isn't free.
    assert (await api.post("/demo/send-batch", json={"count": 51})).status_code == 422
    codes = [(await api.post("/demo/send-batch", json={"count": 1})).status_code for _ in range(6)]
    assert codes == [201, 201, 201, 201, 201, 429]
