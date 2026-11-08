"""Delivery service — query delivery status and attempt history."""
import uuid
from typing import Optional

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.models.delivery import EventDelivery
from app.models.delivery_attempt import DeliveryAttempt


def get_delivery(
    session: Session,
    delivery_id: uuid.UUID,
    tenant_id: uuid.UUID,
) -> Optional[EventDelivery]:
    """Get a delivery record, enforcing tenant ownership."""
    stmt = select(EventDelivery).where(
        EventDelivery.id == delivery_id,
        EventDelivery.tenant_id == tenant_id,
    )
    return session.execute(stmt).scalar_one_or_none()


def get_delivery_attempts(
    session: Session,
    delivery_id: uuid.UUID,
    tenant_id: uuid.UUID,
    offset: int = 0,
    limit: int = 50,
) -> tuple[list[DeliveryAttempt], int]:
    """Get attempt history for a delivery, enforcing tenant ownership.

    Returns:
        Tuple of (attempts, total_count).
    """
    # First verify the delivery belongs to this tenant
    delivery = get_delivery(session, delivery_id, tenant_id)
    if delivery is None:
        return [], 0

    limit = min(limit, 100)

    count_stmt = select(func.count(DeliveryAttempt.id)).where(
        DeliveryAttempt.delivery_id == delivery_id
    )
    total = session.execute(count_stmt).scalar_one()

    stmt = (
        select(DeliveryAttempt)
        .where(DeliveryAttempt.delivery_id == delivery_id)
        .order_by(DeliveryAttempt.attempt_number.asc())
        .offset(offset)
        .limit(limit)
    )
    attempts = list(session.execute(stmt).scalars().all())

    return attempts, total
