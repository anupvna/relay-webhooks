import secrets

from fastapi import APIRouter, Depends, Request

from app.auth import require_admin
from app.schemas import EndpointCreate

router = APIRouter()


@router.post("/endpoints", status_code=201, dependencies=[Depends(require_admin)])
async def create_endpoint(body: EndpointCreate, request: Request):
    """Register a receiver. The secret is shown once; the receiver uses it to verify signatures."""
    row = await request.app.state.pool.fetchrow(
        "INSERT INTO endpoints (url, secret) VALUES ($1, $2) RETURNING id, url, secret, created_at",
        str(body.url),
        secrets.token_hex(32),
    )
    return dict(row)
