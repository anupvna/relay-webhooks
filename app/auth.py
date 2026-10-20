"""Admin API key check.

Public visitors can only touch the demo endpoint. Anything that could make the
server POST to an arbitrary URL needs the X-API-Key header.
"""
import secrets

from fastapi import HTTPException, Request

from app.config import settings


def is_admin(request: Request) -> bool:
    provided = request.headers.get("X-API-Key", "")
    expected = settings.admin_api_key
    # compare_digest takes the same time whether the first or last character is
    # wrong, so an attacker can't guess the key one character at a time.
    return bool(expected) and secrets.compare_digest(provided, expected)


def require_admin(request: Request) -> None:
    if not is_admin(request):
        raise HTTPException(status_code=401, detail="Missing or invalid X-API-Key")
