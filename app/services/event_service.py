"""Event service — transactional event ingestion with outbox pattern.

The core invariant: event creation, delivery record creation, and outbox
entry creation all happen in ONE PostgreSQL transaction. The API returns
HTTP 202 only after this transaction commits successfully.
"""
import uuid
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.models.destination import Destination
from app.models.event import Event
from app.models.delivery import EventDelivery
from app.models.outbox import OutboxEntry

logger = get_logger(__name__)


class EventIngestionError(Exception):
    """Raised when event ingestion fails validation."""
    pass


def ingest_event(
    session: Session,
    tenant_id: uuid.UUID,
    event_type: str,
    payload: dict,
    destination_ids: list[uuid.UUID],
) -> tuple[Event, list[EventDelivery]]:
    """Ingest an event with full transactional guarantees.

    Creates event, delivery records, and outbox entries in a single
    PostgreSQL transaction. Rolls back entirely on any failure.

    Args:
        session: Database session (caller manages the outer transaction boundary).
        tenant_id: Authenticated tenant's ID.
        event_type: Type identifier for the event.
        payload: JSON-serializable event payload.
        destination_ids: List of destination IDs to deliver to.

    Returns:
        Tuple of (event, deliveries).

    Raises:
        EventIngestionError: If destinations are invalid or don't belong to tenant.
    """
    try:
        # Validate all destinations belong to this tenant and are active
        stmt = select(Destination).where(
            Destination.id.in_(destination_ids),
            Destination.tenant_id == tenant_id,
        )
        destinations = list(session.execute(stmt).scalars().all())
        found_ids = {d.id for d in destinations}

        # Check for missing destinations
        missing = [str(did) for did in destination_ids if did not in found_ids]
        if missing:
            raise EventIngestionError(
                f"Destinations not found or not owned by tenant: {', '.join(missing)}"
            )

        # Check for inactive destinations
        inactive = [str(d.id) for d in destinations if not d.is_active]
        if inactive:
            raise EventIngestionError(
                f"Inactive destinations cannot receive events: {', '.join(inactive)}"
            )

        # Create event
        event = Event(
            tenant_id=tenant_id,
            event_type=event_type,
            payload=payload,
        )
        session.add(event)
        session.flush()  # Get the event ID without committing

        # Create delivery records and outbox entries
        deliveries = []
        for dest_id in destination_ids:
            delivery = EventDelivery(
                tenant_id=tenant_id,
                event_id=event.id,
                destination_id=dest_id,
                status="pending",
            )
            session.add(delivery)
            session.flush()  # Get the delivery ID

            outbox = OutboxEntry(
                delivery_id=delivery.id,
                status="pending",
            )
            session.add(outbox)
            deliveries.append(delivery)

        # Commit the entire transaction atomically
        session.commit()

        logger.info(
            "Event ingested successfully",
            extra={
                "tenant_id": str(tenant_id),
                "event_id": str(event.id),
                "delivery_count": len(deliveries),
            },
        )
        return event, deliveries

    except EventIngestionError:
        session.rollback()
        raise
    except Exception as e:
        session.rollback()
        logger.error(
            "Event ingestion failed — transaction rolled back",
            extra={"tenant_id": str(tenant_id), "error": str(e)},
        )
        raise


def get_event(
    session: Session,
    event_id: uuid.UUID,
    tenant_id: uuid.UUID,
) -> Optional[Event]:
    """Get an event with its deliveries, enforcing tenant ownership."""
    stmt = select(Event).where(
        Event.id == event_id,
        Event.tenant_id == tenant_id,
    )
    return session.execute(stmt).scalar_one_or_none()
