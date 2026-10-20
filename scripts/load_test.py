"""Load test: push N events through a flaky receiver and measure what comes out.

What it does:
  1. Sets the demo receiver's failure rate (default 30%) and resets its duplicate counter.
  2. Inserts N events for the demo endpoint in one statement, all at the same moment.
  3. Waits until every event is delivered or dead-lettered.
  4. Prints delivery %, dead %, attempts, p50/p95 end-to-end latency, throughput,
     and duplicate deliveries counted by the receiver itself.

Events are inserted straight into Postgres (not through POST /events) so the test
measures the delivery pipeline, not 2,000 HTTP calls to our own API.

Usage (with the app running locally):
  python scripts/load_test.py --events 2000
Environment:
  DATABASE_URL   same database the app uses (default: local Docker Postgres)
  ADMIN_API_KEY  the app's admin key (needed to reset the receiver's counter)
  BASE_URL       where the app is running (default http://localhost:8000)
"""
import argparse
import asyncio
import os
import statistics
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.db import create_pool  # noqa: E402

EVENT_TYPE = "load.test"


def percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, round(pct / 100 * len(ordered)) - 1))
    return ordered[index]


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--events", type=int, default=2000)
    parser.add_argument("--failure-rate", type=float, default=0.3)
    parser.add_argument("--timeout", type=float, default=900, help="give up after this many seconds")
    args = parser.parse_args()
    if not 1 <= args.events <= 10_000:
        sys.exit("--events must be between 1 and 10,000")

    base_url = os.getenv("BASE_URL", "http://localhost:8000")
    admin_key = os.getenv("ADMIN_API_KEY", "")
    dsn = os.getenv("DATABASE_URL", "postgresql://relay:relay@localhost:5433/relay")

    pool = await create_pool(dsn)
    async with httpx.AsyncClient(base_url=base_url, timeout=10) as api:
        (await api.put("/demo/failure-rate", json={"rate": args.failure_rate})).raise_for_status()
        reset = await api.post("/demo/receiver-stats/reset", headers={"X-API-Key": admin_key})
        if reset.status_code == 401:
            sys.exit("Set ADMIN_API_KEY to the same key the app was started with.")
        reset.raise_for_status()

        demo_id = await pool.fetchval("SELECT id FROM endpoints WHERE is_demo")
        await pool.execute("DELETE FROM events WHERE type = $1", EVENT_TYPE)  # clear previous runs

        print(f"Sending {args.events:,} events at {args.failure_rate:.0%} failure rate...")
        started = time.monotonic()
        await pool.execute(
            """INSERT INTO events (endpoint_id, type, payload)
               SELECT $1, $2, jsonb_build_object('n', n) FROM generate_series(1, $3) AS n""",
            demo_id, EVENT_TYPE, args.events,
        )

        while True:
            open_count = await pool.fetchval(
                "SELECT count(*) FROM events WHERE type = $1 AND status IN ('pending', 'delivering')",
                EVENT_TYPE,
            )
            elapsed = time.monotonic() - started
            print(f"\r  {elapsed:6.1f}s  {args.events - open_count:,}/{args.events:,} finished", end="", flush=True)
            if open_count == 0:
                break
            if elapsed > args.timeout:
                sys.exit(f"\nTimed out with {open_count} events still open.")
            await asyncio.sleep(1)
        print()

        rows = await pool.fetch(
            """SELECT status, attempts,
                      extract(epoch FROM delivered_at - created_at) AS seconds,
                      delivered_at
               FROM events WHERE type = $1""",
            EVENT_TYPE,
        )
        # Duration: from when the events were queued until the very last attempt
        # finished (includes dead-lettered events, not just delivered ones).
        total_attempts, duration = await pool.fetchrow(
            """SELECT count(*),
                      extract(epoch FROM max(a.created_at) - min(e.created_at))::float8
               FROM delivery_attempts a JOIN events e ON e.id = a.event_id
               WHERE e.type = $1""",
            EVENT_TYPE,
        )
        receiver = (await api.get("/demo/receiver-stats")).json()
        await api.put("/demo/failure-rate", json={"rate": 0.2})  # back to the demo default

    await pool.close()

    delivered = [r for r in rows if r["status"] == "delivered"]
    dead = [r for r in rows if r["status"] == "dead"]
    latencies = [float(r["seconds"]) for r in delivered]

    n = len(rows)
    results = [
        ("Events", f"{n:,}"),
        ("Receiver failure rate", f"{args.failure_rate:.0%}"),
        ("Eventually delivered", f"{len(delivered):,} ({100 * len(delivered) / n:.2f}%)"),
        ("Dead-lettered", f"{len(dead):,} ({100 * len(dead) / n:.2f}%)"),
        ("Avg attempts per delivered event", f"{statistics.mean(r['attempts'] for r in delivered):.2f}"),
        ("Total HTTP attempts", f"{total_attempts:,}"),
        ("p50 end-to-end delivery time", f"{percentile(latencies, 50):.2f}s"),
        ("p95 end-to-end delivery time", f"{percentile(latencies, 95):.2f}s"),
        ("Throughput", f"{n / duration:.1f} events/sec ({total_attempts / duration:.1f} attempts/sec)"),
        ("Duplicate deliveries (counted by receiver)", f"{receiver['duplicate_deliveries']}"),
    ]
    width = max(len(k) for k, _ in results)
    print("\nResults")
    for key, value in results:
        print(f"  {key:<{width}}  {value}")

    print("\nMarkdown for the README:\n")
    print("| Metric | Result |\n|---|---|")
    for key, value in results:
        print(f"| {key} | {value} |")


if __name__ == "__main__":
    asyncio.run(main())
