"""Transactional outbox model.

The outbox pattern bridges the dual-write problem between PostgreSQL and Redis.
Event ingestion creates outbox entries in the same transaction as event/delivery
records. A dedicated dispatcher polls for pending entries and publishes Celery
tasks. This guarantees that accepted work is never silently lost, though
duplicate publication remains possible if the dispatcher crashes between
Redis accept and PostgreSQL commit.
"""
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    String, DateTime, Text, ForeignKey, Index, CheckConstraint, UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class OutboxEntry(Base):
    __tablename__ = "outbox_entries"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4
    )
    delivery_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("event_deliveries.id", ondelete="CASCADE"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    published_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )
    last_error: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )

    # Relationships
    delivery: Mapped["EventDelivery"] = relationship(back_populates="outbox_entry")

    __table_args__ = (
        Index("ix_outbox_entries_status", "status"),
        UniqueConstraint(
            "delivery_id",
            name="uq_outbox_entries_delivery_id"
        ),
        CheckConstraint(
            "status IN ('pending', 'published')",
            name="ck_outbox_entries_status"
        ),
    )


from app.models.delivery import EventDelivery  # noqa: E402
