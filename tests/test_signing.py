from app.signing import compute_signature, sign_headers, verify_signature

SECRET = "s3cret"
EVENT_ID = "6f1c2c1e-0000-4000-8000-000000000001"
BODY = b'{"id":"6f1c","type":"order.created","data":{"total":42}}'
NOW = 1_760_000_000


def headers(body=BODY, secret=SECRET, now=NOW):
    return sign_headers(secret, EVENT_ID, body, now=now)


def verify(h, body=BODY, secret=SECRET, now=NOW):
    return verify_signature(
        secret, h["X-Webhook-Id"], h["X-Webhook-Timestamp"], h["X-Webhook-Signature"], body, now=now
    )


def test_valid_signature_passes():
    assert verify(headers())


def test_signature_is_deterministic_hex_sha256():
    sig = compute_signature(SECRET, EVENT_ID, NOW, BODY)
    assert sig == compute_signature(SECRET, EVENT_ID, NOW, BODY)
    assert len(sig) == 64 and all(c in "0123456789abcdef" for c in sig)


def test_tampered_body_fails():
    assert not verify(headers(), body=BODY.replace(b"42", b"4200"))


def test_wrong_secret_fails():
    assert not verify(headers(), secret="not-the-secret")


def test_changed_event_id_fails():
    h = headers()
    h["X-Webhook-Id"] = "6f1c2c1e-0000-4000-8000-000000000002"
    assert not verify(h)


def test_old_timestamp_fails_even_with_valid_signature():
    h = headers(now=NOW - 301)  # signed 5 minutes and 1 second ago
    assert not verify(h, now=NOW)


def test_timestamp_within_tolerance_passes():
    assert verify(headers(now=NOW - 299), now=NOW)


def test_far_future_timestamp_fails():
    assert not verify(headers(now=NOW + 301), now=NOW)


def test_garbage_timestamp_fails():
    h = headers()
    h["X-Webhook-Timestamp"] = "yesterday"
    assert not verify(h)


def test_missing_signature_fails():
    h = headers()
    h["X-Webhook-Signature"] = ""
    assert not verify(h)
