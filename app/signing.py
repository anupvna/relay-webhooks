"""HMAC-SHA256 webhook signatures.

What gets signed: "{event_id}.{timestamp}.{raw_body}"
- event_id ties the signature to one event (also the receiver's de-dup key)
- timestamp lets the receiver reject old requests (replay protection)
- raw_body: any change to the payload breaks the signature
"""
import hashlib
import hmac
import time

TOLERANCE_SECONDS = 300  # receivers reject timestamps more than 5 minutes off


def compute_signature(secret: str, event_id: str, timestamp: int, body: bytes) -> str:
    message = f"{event_id}.{timestamp}.".encode() + body
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def sign_headers(secret: str, event_id: str, body: bytes, now: int | None = None) -> dict:
    timestamp = int(time.time()) if now is None else now
    return {
        "X-Webhook-Id": event_id,
        "X-Webhook-Timestamp": str(timestamp),
        "X-Webhook-Signature": compute_signature(secret, event_id, timestamp, body),
    }


def verify_signature(
    secret: str,
    event_id: str,
    timestamp: str,
    signature: str,
    body: bytes,
    now: int | None = None,
    tolerance: int = TOLERANCE_SECONDS,
) -> bool:
    """What a receiver runs on every incoming webhook."""
    try:
        ts = int(timestamp)
    except (TypeError, ValueError):
        return False
    current = int(time.time()) if now is None else now
    if abs(current - ts) > tolerance:
        return False
    expected = compute_signature(secret, event_id, ts, body)
    # Constant-time compare: doesn't leak how many leading characters matched.
    return hmac.compare_digest(expected, signature or "")
