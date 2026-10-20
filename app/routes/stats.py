from fastapi import APIRouter, Request

router = APIRouter()

THROUGHPUT_WINDOW_SECONDS = 60


@router.get("/stats")
async def stats(request: Request):
    """Dashboard numbers, computed in one query with FILTER clauses."""
    row = await request.app.state.pool.fetchrow(
        """SELECT
               count(*)                                         AS total,
               count(*) FILTER (WHERE status = 'delivered')     AS delivered,
               count(*) FILTER (WHERE status = 'dead')          AS dead,
               count(*) FILTER (WHERE status IN ('pending', 'delivering')) AS in_flight,
               avg(attempts) FILTER (WHERE status = 'delivered') AS avg_attempts,
               count(*) FILTER (
                   WHERE delivered_at > now() - make_interval(secs => $1)
               )                                                AS delivered_recently
           FROM events""",
        THROUGHPUT_WINDOW_SECONDS,
    )
    finished = row["delivered"] + row["dead"]
    return {
        "total": row["total"],
        "delivered": row["delivered"],
        "dead": row["dead"],
        "in_flight": row["in_flight"],
        # Share of *finished* events that were delivered (in-flight ones haven't decided yet).
        "delivered_pct": round(100 * row["delivered"] / finished, 1) if finished else None,
        "avg_attempts": round(float(row["avg_attempts"]), 2) if row["avg_attempts"] else None,
        "events_per_sec": round(row["delivered_recently"] / THROUGHPUT_WINDOW_SECONDS, 2),
    }
