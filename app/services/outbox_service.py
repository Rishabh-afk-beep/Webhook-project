"""Outbox service — claim and mark outbox records."""
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.models.outbox import OutboxEntry

logger = get_logger(__name__)


def claim_pending_entries(
    session: Session,
    batch_size: int = 50,
) -> list[OutboxEntry]:
    """Claim a batch of pending outbox entries using SELECT FOR UPDATE SKIP LOCKED.

    This ensures multiple dispatcher instances don't claim the same rows.
    SKIP LOCKED skips rows locked by other transactions instead of blocking.

    Returns:
        List of claimed outbox entries (still in 'pending' status).
    """
    stmt = (
        select(OutboxEntry)
        .where(OutboxEntry.status == "pending")
        .order_by(OutboxEntry.created_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    entries = list(session.execute(stmt).scalars().all())
    return entries


def mark_published(
    session: Session,
    entry_id: uuid.UUID,
) -> None:
    """Mark an outbox entry as successfully published."""
    mark_published_batch(session, [entry_id])


def mark_published_batch(
    session: Session,
    entry_ids: list[uuid.UUID],
) -> None:
    """Mark multiple outbox entries as successfully published in a single batch."""
    if not entry_ids:
        return
    stmt = (
        update(OutboxEntry)
        .where(OutboxEntry.id.in_(entry_ids))
        .values(
            status="published",
            published_at=datetime.now(timezone.utc),
            last_error=None,
        )
    )
    session.execute(stmt)
    session.commit()


def mark_publish_error(
    session: Session,
    entry_id: uuid.UUID,
    error: str,
) -> None:
    """Record a publish error on an outbox entry without deleting it.

    The entry remains in 'pending' status for retry on the next poll cycle.
    """
    # Truncate error message to prevent unbounded storage
    truncated_error = error[:500] if error else None
    stmt = (
        update(OutboxEntry)
        .where(OutboxEntry.id == entry_id)
        .values(last_error=truncated_error)
    )
    session.execute(stmt)
    session.commit()

    logger.warning(
        "Outbox publish error recorded",
        extra={"outbox_id": str(entry_id), "error": truncated_error},
    )
