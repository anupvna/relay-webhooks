"""FastAPI entry point. Later steps will start the DB pool and workers here."""
from contextlib import asynccontextmanager

from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: (step 2) open DB pool, (step 4) start worker tasks + cleanup task.
    yield
    # Shutdown: stop workers, close pool.


app = FastAPI(title="Relay", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}
