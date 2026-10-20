"""Postgres connection pool, migration runner, and demo endpoint setup."""
import json
import secrets
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import asyncpg

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


async def _init_connection(conn: asyncpg.Connection) -> None:
    # Let us pass Python dicts in and get dicts back for jsonb columns.
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


def clean_dsn(dsn: str) -> str:
    """Drop URL options asyncpg doesn't understand. Neon's connection strings include
    channel_binding=require, which asyncpg would send to the server as an (invalid) setting."""
    parts = urlsplit(dsn)
    query = [(k, v) for k, v in parse_qsl(parts.query) if k != "channel_binding"]
    return urlunsplit(parts._replace(query=urlencode(query)))


async def create_pool(dsn: str, max_size: int = 10) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        clean_dsn(dsn), min_size=1, max_size=max_size, init=_init_connection
    )


async def run_migrations(pool: asyncpg.Pool) -> list[str]:
    """Apply any .sql files in migrations/ that haven't run yet, in name order."""
    applied_now = []
    async with pool.acquire() as conn:
        await conn.execute(
            """CREATE TABLE IF NOT EXISTS schema_migrations (
                   filename   text PRIMARY KEY,
                   applied_at timestamptz NOT NULL DEFAULT now()
               )"""
        )
        done = {r["filename"] for r in await conn.fetch("SELECT filename FROM schema_migrations")}
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            if path.name in done:
                continue
            # Each migration runs in its own transaction: all of it applies, or none.
            async with conn.transaction():
                await conn.execute(path.read_text())
                await conn.execute(
                    "INSERT INTO schema_migrations (filename) VALUES ($1)", path.name
                )
            applied_now.append(path.name)
    return applied_now


async def ensure_demo_endpoint(pool: asyncpg.Pool, url: str) -> asyncpg.Record:
    """Create the demo endpoint once; keep its secret, but update its URL if it changed."""
    return await pool.fetchrow(
        """INSERT INTO endpoints (url, secret, is_demo)
           VALUES ($1, $2, true)
           ON CONFLICT (is_demo) WHERE is_demo
           DO UPDATE SET url = EXCLUDED.url
           RETURNING *""",
        url,
        secrets.token_hex(32),
    )
