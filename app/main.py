"""FastAPI entry point. On startup: DB pool, migrations, demo endpoint, workers."""
import asyncio
import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from app import db
from app.config import settings
from app.routes import demo, endpoints, events, stats
from app.worker import WorkerConfig, cleanup_loop, worker_loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)  # one log line per webhook is plenty


def worker_config() -> WorkerConfig:
    return WorkerConfig(
        batch_size=settings.claim_batch_size,
        lease_seconds=settings.lease_seconds,
        timeout=settings.delivery_timeout,
        max_attempts=settings.max_attempts,
        backoff_base=settings.backoff_base,
        backoff_cap=settings.backoff_cap,
        idle_sleep=settings.idle_sleep,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await db.create_pool(settings.database_url)
    await db.run_migrations(pool)
    demo_ep = await db.ensure_demo_endpoint(pool, f"{settings.public_base_url}/demo/receiver")

    # Shared state that route handlers can reach via request.app.state.
    app.state.pool = pool
    app.state.demo_endpoint_id = demo_ep["id"]
    app.state.demo_secret = demo_ep["secret"]
    app.state.demo = demo.DemoState(settings.demo_failure_rate)

    # One HTTP client shared by all workers: it reuses connections (keep-alive)
    # instead of opening a new TCP/TLS connection for every webhook.
    client = httpx.AsyncClient(limits=httpx.Limits(max_connections=100))
    stop = asyncio.Event()
    cfg = worker_config()
    tasks = [
        asyncio.create_task(worker_loop(f"worker-{i}", pool, client, cfg, stop))
        for i in range(settings.worker_count)
    ]
    tasks.append(
        asyncio.create_task(
            cleanup_loop(pool, demo_ep["id"], settings.demo_retention_minutes, stop)
        )
    )

    yield

    # Graceful shutdown: let workers finish their current batch, then clean up.
    stop.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    await client.aclose()
    await pool.close()


app = FastAPI(title="Relay", lifespan=lifespan)
app.include_router(endpoints.router)
app.include_router(events.router)
app.include_router(stats.router)
app.include_router(demo.router)


@app.get("/health")
async def health():
    # Also checks the database is reachable, not just that the process is up.
    await app.state.pool.fetchval("SELECT 1")
    return {"status": "ok"}
