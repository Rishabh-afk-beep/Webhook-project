"""Dead-Letter Queue service.

Handles listing, retrieving, and replaying dead-lettered deliveries.
"""
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.models.delivery import EventDelivery
from app.models.delivery_attempt import DeliveryAttempt
from app.models.outbox import OutboxEntry

logger = get_logger(__name__)


class DLQError(Exception):
    """Base DLQ service error."""
    pass


class DLQNotFoundError(DLQError):
    """Delivery not found or does not belong to tenant."""
    pass


class DLQNotDeadLetteredError(DLQError):
    """Delivery is not in dead_lettered status."""
    pass


class DLQAlreadyReplayedError(DLQError):
    """Delivery has already been replayed."""
    pass


def list_dead_letters(
    session: Session,
    tenant_id: uuid.UUID,
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[EventDelivery], int]:
    """List dead-lettered deliveries for a tenant, ordered newest first.

    Returns:
        Tuple of (deliveries_with_attempts, total_count).
    """
    base_where = [
        EventDelivery.tenant_id == tenant_id,
        EventDelivery.status == "dead_lettered",
    ]

    count_stmt = select(func.count(EventDelivery.id)).where(*base_where)
    total = session.execute(count_stmt).scalar_one()

    stmt = (
        select(EventDelivery)
        .where(*base_where)
        .order_by(EventDelivery.dead_lettered_at.desc().nullslast())
        .offset(offset)
        .limit(limit)
    )
    deliveries = list(session.execute(stmt).scalars().all())

    # Eagerly load attempts for each delivery
    for delivery in deliveries:
        _ = delivery.attempts  # triggers lazy load while session is open

    return deliveries, total


def get_dead_letter(
    session: Session,
    tenant_id: uuid.UUID,
    delivery_id: uuid.UUID,
) -> EventDelivery:
    """Get a single dead-lettered delivery with attempt history.

    Raises:
        DLQNotFoundError: if not found or wrong tenant.
        DLQNotDeadLetteredError: if not in dead_lettered status.
    """
    stmt = select(EventDelivery).where(
        EventDelivery.id == delivery_id,
        EventDelivery.tenant_id == tenant_id,
    )
    delivery = session.execute(stmt).scalar_one_or_none()

    if delivery is None:
        raise DLQNotFoundError(f"Delivery {delivery_id} not found")

    if delivery.status != "dead_lettered":
        raise DLQNotDeadLetteredError(
            f"Delivery {delivery_id} is not dead-lettered (status={delivery.status})"
        )

    # Eagerly load attempts
    _ = delivery.attempts

    return delivery


def replay_delivery(
    session: Session,
    tenant_id: uuid.UUID,
    delivery_id: uuid.UUID,
) -> EventDelivery:
    """Create a new EventDelivery as a replay of a dead-lettered one.

    The original delivery and its attempt history are preserved unchanged.
    A new delivery is created (status='pending') linked via replay_of_delivery_id.
    The original's replayed_at is set to now to prevent double-replay.

    Returns:
        The newly created EventDelivery.

    Raises:
        DLQNotFoundError: if not found or wrong tenant.
        DLQNotDeadLetteredError: if not in dead_lettered status.
        DLQAlreadyReplayedError: if already replayed.
    """
    # Lock the row to prevent concurrent replay
    stmt = (
        select(EventDelivery)
        .where(
            EventDelivery.id == delivery_id,
            EventDelivery.tenant_id == tenant_id,
        )
        .with_for_update()
    )
    delivery = session.execute(stmt).scalar_one_or_none()

    if delivery is None:
        raise DLQNotFoundError(f"Delivery {delivery_id} not found")

    if delivery.status != "dead_lettered":
        raise DLQNotDeadLetteredError(
            f"Delivery {delivery_id} is not dead-lettered (status={delivery.status})"
        )

    # Idempotency: reject if already replayed
    if delivery.replayed_at is not None:
        raise DLQAlreadyReplayedError(
            f"Delivery {delivery_id} has already been replayed"
        )

    # Create the new delivery
    new_delivery = EventDelivery(
        tenant_id=delivery.tenant_id,
        event_id=delivery.event_id,
        destination_id=delivery.destination_id,
        status="pending",
        replay_of_delivery_id=delivery.id,
    )
    session.add(new_delivery)
    session.flush()  # Get the new delivery's ID

    # Create outbox entry so the dispatcher picks it up
    outbox = OutboxEntry(
        delivery_id=new_delivery.id,
        status="pending",
    )
    session.add(outbox)

    # Mark original as replayed
    delivery.replayed_at = datetime.now(timezone.utc)
    delivery.updated_at = datetime.now(timezone.utc)

    session.commit()

    logger.info(
        "Dead-lettered delivery replayed",
        extra={
            "original_delivery_id": str(delivery_id),
            "new_delivery_id": str(new_delivery.id),
            "tenant_id": str(tenant_id),
        },
    )
    return new_delivery
