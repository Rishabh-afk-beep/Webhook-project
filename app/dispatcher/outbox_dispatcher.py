"""Outbox dispatcher — polls PostgreSQL for pending outbox records and publishes Celery tasks.

This process bridges the gap between PostgreSQL (source of truth) and Redis (Celery broker).
It runs as a dedicated service and uses SELECT FOR UPDATE SKIP LOCKED to support
multiple concurrent instances without double-claiming.

Known limitation: If the dispatcher crashes after Redis accepts a task but before
PostgreSQL marks the outbox entry as 'published', the same task may be published
again on the next cycle. The delivery worker handles this via duplicate-task guards.
"""
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.core.config import get_settings
from app.core.logging import setup_logging, get_logger
from app.db.session import create_session
from app.models.delivery import EventDelivery
from app.services.outbox_service import (
    claim_pending_entries,
    mark_published,
    mark_published_batch,
    mark_publish_error,
)
from app.workers.tasks import deliver_webhook

setup_logging()
logger = get_logger(__name__)

_running = True


def _handle_shutdown(signum, frame):
    """Handle graceful shutdown signals."""
    global _running
    logger.info("Shutdown signal received, stopping dispatcher...")
    _running = False


def recover_stuck_processing_deliveries(session, lease_timeout_seconds: int = 30) -> int:
    """Find deliveries stuck in 'processing' longer than lease timeout and re-queue them.

    This ensures that even if a worker crashes AND the task acknowledgment was dropped,
    the transactional database watchdog will re-publish the delivery to Celery.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=lease_timeout_seconds)
    bind = session.get_bind()
    dialect_name = getattr(bind, "dialect", None) and bind.dialect.name
    if cutoff.tzinfo is not None and dialect_name != "postgresql":
        cutoff = cutoff.replace(tzinfo=None)
    stmt = (
        select(EventDelivery)
        .where(
            EventDelivery.status == "processing",
            EventDelivery.updated_at <= cutoff,
        )
        .limit(20)
    )
    stuck_deliveries = list(session.execute(stmt).scalars().all())
    reclaimed = 0
    for delivery in stuck_deliveries:
        try:
            deliver_webhook.delay(str(delivery.id))
            reclaimed += 1
            logger.warning(
                "Watchdog re-published stuck processing delivery",
                extra={"delivery_id": str(delivery.id)},
            )
        except Exception as e:
            logger.error(
                "Watchdog failed to re-publish stuck delivery",
                extra={"delivery_id": str(delivery.id), "error": str(e)},
            )
    return reclaimed


def process_outbox_batch(session, batch_size: int) -> int:
    """Process a single batch of pending outbox entries.

    Returns the number of entries successfully published.
    """
    entries = claim_pending_entries(session, batch_size=batch_size)
    if not entries:
        session.commit()
        return 0

    logger.info(f"Claimed {len(entries)} outbox entries for publishing")
    published_ids = []

    for entry in entries:
        try:
            deliver_webhook.delay(str(entry.delivery_id))
            published_ids.append(entry.id)
        except Exception as e:
            logger.error(
                "Failed to publish outbox entry",
                extra={
                    "outbox_id": str(entry.id),
                    "delivery_id": str(entry.delivery_id),
                    "error": str(e),
                },
            )
            try:
                mark_publish_error(session, entry.id, str(e))
            except Exception as inner_e:
                logger.error(
                    "Failed to record publish error",
                    extra={"outbox_id": str(entry.id), "error": str(inner_e)},
                )

    if published_ids:
        mark_published_batch(session, published_ids)

    return len(published_ids)


def run_dispatcher() -> None:
    """Main dispatcher loop.

    Continuously polls for pending outbox entries, publishes Celery tasks,
    and marks entries as published.
    """
    global _running
    settings = get_settings()

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    logger.info(
        "Outbox dispatcher starting",
        extra={
            "poll_interval": settings.outbox_poll_interval_seconds,
            "batch_size": settings.outbox_batch_size,
        },
    )

    while _running:
        session = create_session()
        published = 0
        try:
            published = process_outbox_batch(session, settings.outbox_batch_size)
            recover_stuck_processing_deliveries(session, settings.delivery_processing_lease_timeout_seconds)
        except Exception as e:
            logger.error(
                "Dispatcher poll cycle failed",
                extra={"error": str(e)},
                exc_info=True,
            )
            session.rollback()
        finally:
            session.close()

        # If a full batch was processed, immediately poll next batch; otherwise sleep
        if published < settings.outbox_batch_size and _running:
            time.sleep(settings.outbox_poll_interval_seconds)

    logger.info("Outbox dispatcher stopped")


if __name__ == "__main__":
    run_dispatcher()
