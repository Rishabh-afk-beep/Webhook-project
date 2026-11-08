"""Destination service — business logic for destination management."""
import uuid
from typing import Optional

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.core.security import validate_destination_url, generate_signing_secret
from app.models.destination import Destination

logger = get_logger(__name__)


def create_destination(
    session: Session,
    tenant_id: uuid.UUID,
    name: str,
    url: str,
) -> Destination:
    """Create a new webhook destination for a tenant.

    Validates the URL for SSRF safety, generates a signing secret,
    and persists the destination.

    Raises:
        ValueError: If the URL fails validation.
    """
    # Validate URL
    is_valid, error = validate_destination_url(url)
    if not is_valid:
        raise ValueError(f"Invalid destination URL: {error}")

    signing_secret = generate_signing_secret()

    destination = Destination(
        tenant_id=tenant_id,
        name=name,
        url=url,
        signing_secret=signing_secret,
        is_active=True,
    )
    session.add(destination)
    session.commit()
    session.refresh(destination)

    logger.info(
        "Destination created",
        extra={"tenant_id": str(tenant_id), "destination_id": str(destination.id)},
    )
    return destination


def list_destinations(
    session: Session,
    tenant_id: uuid.UUID,
    offset: int = 0,
    limit: int = 50,
) -> tuple[list[Destination], int]:
    """List destinations for a tenant with pagination.

    Returns:
        Tuple of (destinations, total_count).
    """
    limit = min(limit, 100)

    count_stmt = select(func.count(Destination.id)).where(
        Destination.tenant_id == tenant_id
    )
    total = session.execute(count_stmt).scalar_one()

    stmt = (
        select(Destination)
        .where(Destination.tenant_id == tenant_id)
        .order_by(Destination.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    destinations = list(session.execute(stmt).scalars().all())

    return destinations, total


def get_destination(
    session: Session,
    destination_id: uuid.UUID,
    tenant_id: uuid.UUID,
) -> Optional[Destination]:
    """Get a single destination, enforcing tenant ownership."""
    stmt = select(Destination).where(
        Destination.id == destination_id,
        Destination.tenant_id == tenant_id,
    )
    return session.execute(stmt).scalar_one_or_none()
