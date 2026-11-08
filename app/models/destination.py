"""Destination model."""
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import String, Boolean, DateTime, Float, Integer, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Destination(Base):
    __tablename__ = "destinations"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    signing_secret: Mapped[Optional[str]] = mapped_column(
        String(128), nullable=True, default=None,
        comment="HMAC signing secret; never returned after creation"
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=False
    )
    # Per-destination rate limiting (NULL = use global default from settings)
    rate_limit_per_second: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, default=None
    )
    rate_limit_burst: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, default=None
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
    tenant: Mapped["Tenant"] = relationship(back_populates="destinations")
    deliveries: Mapped[list["EventDelivery"]] = relationship(back_populates="destination", lazy="select")

    __table_args__ = (
        Index("ix_destinations_tenant_id", "tenant_id"),
    )


from app.models.tenant import Tenant  # noqa: E402
from app.models.delivery import EventDelivery  # noqa: E402
