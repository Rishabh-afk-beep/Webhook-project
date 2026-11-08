"""Test receiver — a development/testing-only FastAPI application.

Provides endpoints that simulate various webhook destination behaviors:
- Success (200)
- Failure (500)
- Slow response (configurable delay)
- Rate limited (429)

NEVER expose this as a public production service.
"""
import os
import time
import threading
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Request, Response, status

app = FastAPI(title="Webhook Test Receiver", version="0.1.0")

# Thread-safe received request storage
_received_lock = threading.Lock()
_received_requests: list[dict[str, Any]] = []
_total_received_count: int = 0

SLOW_DELAY_SECONDS = float(os.environ.get("TEST_RECEIVER_SLOW_DELAY", "4"))


def _record_request(request: Request, body: bytes, endpoint: str) -> dict:
    """Record an incoming webhook request for test inspection."""
    global _total_received_count
    record = {
        "endpoint": endpoint,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "method": request.method,
        "headers": dict(request.headers),
        "body": body.decode("utf-8", errors="replace")[:4096],
        "path": str(request.url.path),
    }
    with _received_lock:
        _total_received_count += 1
        _received_requests.append(record)
        # Keep bounded: only last 5000 requests in memory buffer
        if len(_received_requests) > 5000:
            _received_requests.pop(0)
    return record


@app.get("/health")
def health():
    """Health check for the test receiver."""
    return {"status": "ok", "service": "test-receiver"}


@app.post("/webhooks/success")
async def webhook_success(request: Request):
    """Accept a webhook and return 200 OK."""
    body = await request.body()
    _record_request(request, body, "success")
    return {"status": "received"}


@app.post("/webhooks/failure")
async def webhook_failure(request: Request):
    """Simulate a server error — returns 500."""
    body = await request.body()
    _record_request(request, body, "failure")
    return Response(
        content='{"error": "internal server error"}',
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        media_type="application/json",
    )


@app.post("/webhooks/slow")
async def webhook_slow(request: Request):
    """Simulate a slow response. Delay is configurable via TEST_RECEIVER_SLOW_DELAY."""
    body = await request.body()
    _record_request(request, body, "slow")
    time.sleep(SLOW_DELAY_SECONDS)
    return {"status": "received_slowly"}


@app.post("/webhooks/rate-limited")
async def webhook_rate_limited(request: Request):
    """Simulate rate limiting — returns 429 with Retry-After header."""
    body = await request.body()
    _record_request(request, body, "rate-limited")
    return Response(
        content='{"error": "too many requests"}',
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        media_type="application/json",
        headers={"Retry-After": "60"},
    )


@app.get("/requests")
def get_received_requests(limit: int = 50):
    """Inspect received webhook requests (for testing)."""
    with _received_lock:
        return {
            "total": _total_received_count,
            "total_count": _total_received_count,
            "buffer_size": len(_received_requests),
            "requests": _received_requests[-limit:],
        }


@app.get("/delivery-audit")
def get_delivery_audit():
    """Audit unique deliveries versus duplicate requests received."""
    with _received_lock:
        delivery_ids = []
        for req in _received_requests:
            h = {k.lower(): v for k, v in req.get("headers", {}).items()}
            del_id = h.get("x-webhook-delivery-id")
            if del_id:
                delivery_ids.append(del_id)

        unique_ids = set(delivery_ids)
        duplicates = len(delivery_ids) - len(unique_ids)
        return {
            "total_received_count": _total_received_count,
            "recorded_requests_in_buffer": len(delivery_ids),
            "unique_delivery_ids_count": len(unique_ids),
            "duplicate_requests_count": duplicates,
        }


@app.delete("/requests")
def clear_received_requests():
    """Clear received request log."""
    global _total_received_count
    with _received_lock:
        _total_received_count = 0
        _received_requests.clear()
    return {"status": "cleared"}
