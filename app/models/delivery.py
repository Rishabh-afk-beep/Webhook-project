"""Event delivery model."""
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    String, Integer, Text, DateTime, ForeignKey, Index, CheckConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class EventDelivery(Base):
    """Represents the delivery of one event to one destination.

    A single event sent to N destinations creates N EventDelivery rows.
    """
    __tablename__ = "event_deliveries"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    event_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("events.id", ondelete="CASCADE"), nullable=False
    )
    destination_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("destinations.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending"
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )
    last_http_status: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, default=None
    )
    last_error: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )
    next_attempt_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )
    dead_lettered_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )
    replayed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )
    replay_of_delivery_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("event_deliveries.id", ondelete="SET NULL"),
        nullable=True,
        default=None,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # Relationships
    event: Mapped["Event"] = relationship(back_populates="deliveries")
    destination: Mapped["Destination"] = relationship(back_populates="deliveries")
    attempts: Mapped[list["DeliveryAttempt"]] = relationship(
        back_populates="delivery", lazy="select", order_by="DeliveryAttempt.attempt_number"
    )
    outbox_entry: Mapped[Optional["OutboxEntry"]] = relationship(
        back_populates="delivery", uselist=False, lazy="select"
    )
    # Self-referential: the original delivery this one replays
    replayed_from: Mapped[Optional["EventDelivery"]] = relationship(
        "EventDelivery",
        foreign_keys=[replay_of_delivery_id],
        remote_side="EventDelivery.id",
        uselist=False,
    )

    __table_args__ = (
        Index("ix_event_deliveries_event_id", "event_id"),
        Index("ix_event_deliveries_tenant_id", "tenant_id"),
        Index("ix_event_deliveries_status", "status"),
        # Partial index for retry scheduling queries (PostgreSQL only; ignored by SQLite)
        Index(
            "ix_event_deliveries_next_attempt_at",
            "status",
            "next_attempt_at",
            postgresql_where="status = 'retry_scheduled'",
        ),
        # Partial index for DLQ queries (PostgreSQL only; ignored by SQLite)
        Index(
            "ix_event_deliveries_dead_lettered",
            "tenant_id",
            "dead_lettered_at",
            postgresql_where="status = 'dead_lettered'",
        ),
        CheckConstraint(
            "status IN ('pending', 'processing', 'retry_scheduled', 'succeeded', 'failed', 'dead_lettered')",
            name="ck_event_deliveries_status"
        ),
        CheckConstraint(
            "attempt_count >= 0",
            name="ck_event_deliveries_attempt_count_non_negative"
        ),
    )


from app.models.event import Event  # noqa: E402
from app.models.destination import Destination  # noqa: E402
from app.models.delivery_attempt import DeliveryAttempt  # noqa: E402
from app.models.outbox import OutboxEntry  # noqa: E402
