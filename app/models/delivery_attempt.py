"""Delivery attempt model."""
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    String, Integer, DateTime, Text, ForeignKey, UniqueConstraint, CheckConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class DeliveryAttempt(Base):
    """Records a single HTTP request attempt for a delivery.

    Each actual HTTP request creates exactly one DeliveryAttempt row.
    Duplicate-job discards do NOT create attempt rows.
    """
    __tablename__ = "delivery_attempts"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4
    )
    delivery_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("event_deliveries.id", ondelete="CASCADE"), nullable=False
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )
    outcome: Mapped[str] = mapped_column(String(20), nullable=False)
    http_status: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, default=None
    )
    response_time_ms: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, default=None
    )
    error_message: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )
    error_category: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True, default=None
    )

    # Relationships
    delivery: Mapped["EventDelivery"] = relationship(back_populates="attempts")

    __table_args__ = (
        UniqueConstraint(
            "delivery_id", "attempt_number",
            name="uq_delivery_attempts_delivery_id_attempt_number"
        ),
        CheckConstraint(
            "outcome IN ('succeeded', 'failed')",
            name="ck_delivery_attempts_outcome"
        ),
    )


from app.models.delivery import EventDelivery  # noqa: E402
