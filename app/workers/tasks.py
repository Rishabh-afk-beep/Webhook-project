"""Celery task for webhook delivery.

Processes one delivery at a time. Handles duplicate tasks, concurrent
delivery prevention, HMAC signing, attempt recording, exponential-backoff
retries, and dead-lettering after max attempts exhausted.
"""
import json
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import select

from app.core.config import get_settings
from app.core.logging import get_logger
from app.core.security import compute_webhook_signature
from app.db.session import create_session
from app.models.delivery import EventDelivery
from app.models.delivery_attempt import DeliveryAttempt
from app.models.destination import Destination
from app.models.event import Event
from app.core.circuit_breaker import get_circuit_breaker, CircuitState
from app.core.rate_limiter import get_rate_limiter
from app.core.metrics import (
    DELIVERIES_TOTAL,
    DELIVERY_ATTEMPTS_TOTAL,
    DELIVERY_DURATION_SECONDS,
    DELIVERY_RETRIES_TOTAL,
    DEAD_LETTERS_TOTAL,
    RATE_LIMIT_DEFERRED_TOTAL,
    CIRCUIT_BREAKER_DEFERRED_TOTAL,
)
from app.workers.celery_app import celery_app
from app.workers.retry_policy import (
    is_retryable_status,
    is_retryable_exception,
    compute_backoff_seconds,
    get_error_category,
)

logger = get_logger(__name__)

# Maximum length for stored error messages
MAX_ERROR_LENGTH = 500

# Statuses that allow the worker to begin processing
ELIGIBLE_STATUSES = {"pending", "retry_scheduled"}


@celery_app.task(
    name="deliver_webhook",
    bind=True,
    max_retries=0,  # Retries managed manually via apply_async
)
def deliver_webhook(self, delivery_id: str) -> dict:
    """Execute a single webhook delivery with retry and dead-letter support.

    Flow:
    1. Load and lock the delivery row.
    2. Verify the delivery is eligible (pending or retry_scheduled).
    3. Atomically transition to 'processing'.
    4. Create a delivery attempt record.
    5. Send the HTTP POST with optional HMAC signature.
    6. Record the outcome.
    7. Apply retry policy or mark terminal status (succeeded / failed /
       retry_scheduled / dead_lettered).

    Duplicate task handling:
    - If the delivery is already 'succeeded', 'processing', 'failed', or
      'dead_lettered', the task is discarded without creating an attempt.
    - Row-level locking (SELECT FOR UPDATE) prevents two workers from
      processing the same delivery concurrently.
    """
    delivery_uuid = uuid.UUID(delivery_id)
    session = create_session()
    settings = get_settings()

    try:
        # --- Step 1: Load and lock the delivery ---
        stmt = (
            select(EventDelivery)
            .where(EventDelivery.id == delivery_uuid)
            .with_for_update()
        )
        delivery = session.execute(stmt).scalar_one_or_none()

        if delivery is None:
            logger.warning(
                "Delivery not found - discarding task",
                extra={"delivery_id": delivery_id},
            )
            return {"status": "discarded", "reason": "delivery_not_found"}

        # --- Step 2: Eligibility check (duplicate / stale task guard) ---
        is_reclaimable_processing = False
        if delivery.status == "processing":
            updated_at = delivery.updated_at
            if updated_at is not None:
                if updated_at.tzinfo is None:
                    updated_at = updated_at.replace(tzinfo=timezone.utc)
                elapsed = (datetime.now(timezone.utc) - updated_at).total_seconds()
            else:
                elapsed = settings.delivery_processing_lease_timeout_seconds

            if elapsed >= settings.delivery_processing_lease_timeout_seconds:
                is_reclaimable_processing = True
                logger.warning(
                    "Reclaiming stuck processing delivery after worker crash lease expiration",
                    extra={
                        "delivery_id": delivery_id,
                        "elapsed_seconds": elapsed,
                        "lease_timeout": settings.delivery_processing_lease_timeout_seconds,
                    },
                )

        if delivery.status not in ELIGIBLE_STATUSES and not is_reclaimable_processing:
            reason_map = {
                "succeeded": "already_succeeded",
                "dead_lettered": "dead_lettered",
                "processing": "already_processing",
                "failed": f"ineligible_status_{delivery.status}",
            }
            reason = reason_map.get(delivery.status, f"ineligible_status_{delivery.status}")
            logger.info(
                "Delivery not eligible for processing - discarding",
                extra={"delivery_id": delivery_id, "status": delivery.status},
            )
            session.rollback()
            return {"status": "discarded", "reason": reason}

        # --- Step 3: Transition to 'processing' (renew lease) ---
        delivery.status = "processing"
        delivery.updated_at = datetime.now(timezone.utc)
        session.commit()

        # --- Load related entities (outside the lock transaction) ---
        event = session.execute(
            select(Event).where(Event.id == delivery.event_id)
        ).scalar_one_or_none()

        destination = session.execute(
            select(Destination).where(Destination.id == delivery.destination_id)
        ).scalar_one_or_none()

        if event is None or destination is None:
            logger.error(
                "Event or destination missing for delivery",
                extra={"delivery_id": delivery_id},
            )
            delivery.status = "failed"
            delivery.last_error = "Missing event or destination record"
            session.commit()
            return {"status": "failed", "reason": "missing_records"}

        if not destination.is_active:
            logger.info(
                "Destination is inactive - marking delivery failed",
                extra={"delivery_id": delivery_id, "destination_id": str(destination.id)},
            )
            delivery.status = "failed"
            delivery.last_error = "Destination is inactive"
            session.commit()
            return {"status": "failed", "reason": "destination_inactive"}

        # --- Pre-flight Check A: Circuit Breaker ---
        circuit_breaker = get_circuit_breaker()
        cb_allowed, cb_state, cb_delay = circuit_breaker.can_execute(str(destination.id))
        if not cb_allowed:
            CIRCUIT_BREAKER_DEFERRED_TOTAL.inc()
            delivery.status = "retry_scheduled"
            delivery.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=cb_delay)
            session.commit()
            deliver_webhook.apply_async(args=[delivery_id], countdown=cb_delay)
            logger.info(
                "Delivery deferred due to open circuit breaker",
                extra={
                    "delivery_id": delivery_id,
                    "destination_id": str(destination.id),
                    "circuit_state": cb_state.value,
                    "retry_after_seconds": cb_delay,
                },
            )
            return {
                "status": "retry_scheduled",
                "reason": "circuit_open",
                "next_attempt_in_seconds": cb_delay,
            }

        # --- Pre-flight Check B: Destination Rate Limiting ---
        rate_limiter = get_rate_limiter()
        rate_per_sec = (
            destination.rate_limit_per_second
            if destination.rate_limit_per_second is not None
            else settings.default_rate_limit_per_second
        )
        burst_cap = (
            destination.rate_limit_burst
            if destination.rate_limit_burst is not None
            else settings.default_rate_limit_burst
        )
        rl_allowed, rl_retry_after_ms = rate_limiter.check_rate_limit(
            str(destination.id),
            rate_per_second=rate_per_sec,
            burst=burst_cap,
        )
        if not rl_allowed:
            RATE_LIMIT_DEFERRED_TOTAL.inc()
            rl_delay_sec = max(1, int(rl_retry_after_ms / 1000) + 1)
            delivery.status = "retry_scheduled"
            delivery.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=rl_delay_sec)
            session.commit()
            deliver_webhook.apply_async(args=[delivery_id], countdown=rl_delay_sec)
            logger.info(
                "Delivery deferred due to destination rate limit",
                extra={
                    "delivery_id": delivery_id,
                    "destination_id": str(destination.id),
                    "retry_after_ms": rl_retry_after_ms,
                },
            )
            return {
                "status": "retry_scheduled",
                "reason": "rate_limited",
                "next_attempt_in_seconds": rl_delay_sec,
            }

        # --- Step 4: Create attempt record ---
        attempt_number = delivery.attempt_count + 1
        attempt = DeliveryAttempt(
            delivery_id=delivery.id,
            attempt_number=attempt_number,
            started_at=datetime.now(timezone.utc),
            outcome="failed",  # Default; updated on success
        )
        session.add(attempt)
        session.flush()

        # --- Step 5: Build and send the HTTP request ---
        settings = get_settings()

        webhook_body = json.dumps({
            "event_id": str(event.id),
            "event_type": event.event_type,
            "payload": event.payload,
        }, separators=(",", ":"), sort_keys=True)

        timestamp = int(time.time())

        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Event-Id": str(event.id),
            "X-Webhook-Delivery-Id": str(delivery.id),
            "X-Webhook-Event-Type": event.event_type,
            "X-Webhook-Timestamp": str(timestamp),
        }

        if destination.signing_secret:
            signature = compute_webhook_signature(
                payload_bytes=webhook_body.encode("utf-8"),
                signing_secret=destination.signing_secret,
                timestamp=timestamp,
            )
            headers["X-Webhook-Signature"] = f"sha256={signature}"

        # Track outcome details for retry policy decision
        http_failed = False
        http_exception: Exception | None = None
        http_status: int | None = None

        start_time = time.monotonic()
        try:
            with httpx.Client(
                timeout=httpx.Timeout(
                    connect=settings.delivery_connect_timeout_seconds,
                    read=settings.delivery_read_timeout_seconds,
                    write=settings.delivery_connect_timeout_seconds,
                    pool=settings.delivery_connect_timeout_seconds,
                ),
            ) as client:
                response = client.post(
                    destination.url,
                    content=webhook_body,
                    headers=headers,
                )
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            DELIVERY_DURATION_SECONDS.observe(elapsed_ms / 1000.0)

            attempt.completed_at = datetime.now(timezone.utc)
            attempt.http_status = response.status_code
            attempt.response_time_ms = elapsed_ms
            http_status = response.status_code

            if 200 <= response.status_code < 300:
                attempt.outcome = "succeeded"
                attempt.error_category = get_error_category(None, response.status_code)
                delivery.status = "succeeded"
                delivery.last_http_status = response.status_code
                delivery.last_error = None
                circuit_breaker.record_success(str(destination.id))
                DELIVERY_ATTEMPTS_TOTAL.labels(outcome="succeeded", error_category="success").inc()
                DELIVERIES_TOTAL.labels(status="succeeded").inc()
                logger.info(
                    "Delivery succeeded",
                    extra={
                        "delivery_id": delivery_id,
                        "http_status": response.status_code,
                        "response_time_ms": elapsed_ms,
                    },
                )
            else:
                http_failed = True
                attempt.outcome = "failed"
                attempt.error_message = _truncate(f"HTTP {response.status_code}")
                attempt.error_category = get_error_category(None, response.status_code)
                delivery.last_http_status = response.status_code
                delivery.last_error = f"HTTP {response.status_code}"
                logger.warning(
                    "Delivery failed - non-2xx response",
                    extra={
                        "delivery_id": delivery_id,
                        "http_status": response.status_code,
                    },
                )

        except httpx.TimeoutException as e:
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            DELIVERY_DURATION_SECONDS.observe(elapsed_ms / 1000.0)
            http_failed = True
            http_exception = e
            attempt.completed_at = datetime.now(timezone.utc)
            attempt.outcome = "failed"
            attempt.response_time_ms = elapsed_ms
            attempt.error_message = _truncate(f"Timeout: {type(e).__name__}")
            attempt.error_category = get_error_category(e, None)
            delivery.last_error = "Request timed out"
            logger.warning("Delivery failed - timeout", extra={"delivery_id": delivery_id})

        except httpx.ConnectError as e:
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            DELIVERY_DURATION_SECONDS.observe(elapsed_ms / 1000.0)
            http_failed = True
            http_exception = e
            attempt.completed_at = datetime.now(timezone.utc)
            attempt.outcome = "failed"
            attempt.response_time_ms = elapsed_ms
            attempt.error_message = _truncate(f"Connection error: {type(e).__name__}")
            attempt.error_category = get_error_category(e, None)
            delivery.last_error = "Connection failed"
            logger.warning("Delivery failed - connection error", extra={"delivery_id": delivery_id})

        except Exception as e:
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            DELIVERY_DURATION_SECONDS.observe(elapsed_ms / 1000.0)
            http_failed = True
            http_exception = e
            attempt.completed_at = datetime.now(timezone.utc)
            attempt.outcome = "failed"
            attempt.response_time_ms = elapsed_ms
            attempt.error_message = _truncate(f"Error: {type(e).__name__}")
            attempt.error_category = get_error_category(e, None)
            delivery.last_error = _truncate(str(e))
            logger.error(
                "Delivery failed - unexpected error",
                extra={"delivery_id": delivery_id, "error": str(e)},
            )

        # --- Step 7: Apply retry policy & update circuit breaker ---
        delivery.attempt_count = attempt_number
        delivery.updated_at = datetime.now(timezone.utc)

        if http_failed:
            cat = attempt.error_category or "unknown"
            DELIVERY_ATTEMPTS_TOTAL.labels(outcome="failed", error_category=cat).inc()
            is_qualifying = (http_exception is not None) or (http_status is not None and http_status >= 500)
            circuit_breaker.record_failure(str(destination.id), is_qualifying_failure=is_qualifying)

            should_retry = (
                (http_exception is not None and is_retryable_exception(http_exception))
                or (http_status is not None and is_retryable_status(http_status))
            )

            if should_retry and attempt_number < settings.retry_max_attempts:
                # Schedule a retry with exponential backoff + jitter
                delay = compute_backoff_seconds(
                    attempt_number=attempt_number,
                    base_delay=settings.retry_base_delay_seconds,
                    max_delay=settings.retry_max_delay_seconds,
                    jitter_enabled=settings.retry_jitter_enabled,
                )
                delivery.status = "retry_scheduled"
                delivery.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
                DELIVERIES_TOTAL.labels(status="retry_scheduled").inc()
                DELIVERY_RETRIES_TOTAL.inc()
                session.commit()

                # Schedule the retry via Celery (mocked in tests)
                deliver_webhook.apply_async(
                    args=[delivery_id],
                    countdown=int(delay) + 1,
                )
                logger.info(
                    "Delivery retry scheduled",
                    extra={
                        "delivery_id": delivery_id,
                        "attempt_number": attempt_number,
                        "next_attempt_in_seconds": delay,
                    },
                )
                return {
                    "status": "retry_scheduled",
                    "attempt_number": attempt_number,
                    "next_attempt_in_seconds": delay,
                    "http_status": attempt.http_status,
                }
            elif should_retry and attempt_number >= settings.retry_max_attempts:
                # Exhausted all attempts — dead-letter
                delivery.status = "dead_lettered"
                delivery.dead_lettered_at = datetime.now(timezone.utc)
                DELIVERIES_TOTAL.labels(status="dead_lettered").inc()
                DEAD_LETTERS_TOTAL.inc()
                logger.warning(
                    "Delivery dead-lettered after max attempts",
                    extra={
                        "delivery_id": delivery_id,
                        "attempt_count": attempt_number,
                    },
                )
            else:
                # Non-retryable failure (e.g. HTTP 400)
                delivery.status = "failed"
                DELIVERIES_TOTAL.labels(status="failed").inc()
                logger.info(
                    "Delivery failed with non-retryable error",
                    extra={
                        "delivery_id": delivery_id,
                        "http_status": http_status,
                    },
                )

        session.commit()

        return {
            "status": delivery.status,
            "attempt_number": attempt_number,
            "http_status": attempt.http_status,
        }

    except Exception as e:
        session.rollback()
        logger.error(
            "Delivery task encountered an unexpected error",
            extra={"delivery_id": delivery_id, "error": str(e)},
            exc_info=True,
        )
        raise
    finally:
        session.close()


def _truncate(text: str, max_length: int = MAX_ERROR_LENGTH) -> str:
    """Truncate text to a bounded length."""
    if len(text) > max_length:
        return text[:max_length - 3] + "..."
    return text
