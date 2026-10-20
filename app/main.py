"""FastAPI entry point: opens the DB pool and prepares the schema on startup."""
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import db
from app.config import settings
from app.routes import endpoints, events, stats


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await db.create_pool(settings.database_url)
    await db.run_migrations(pool)
    demo = await db.ensure_demo_endpoint(pool, f"{settings.public_base_url}/demo/receiver")

    # Shared state that route handlers (and later the workers) can reach.
    app.state.pool = pool
    app.state.demo_endpoint_id = demo["id"]

    yield

    await pool.close()


app = FastAPI(title="Relay", lifespan=lifespan)
app.include_router(endpoints.router)
app.include_router(events.router)
app.include_router(stats.router)


@app.get("/health")
async def health():
    # Also checks the database is reachable, not just that the process is up.
    await app.state.pool.fetchval("SELECT 1")
    return {"status": "ok"}
