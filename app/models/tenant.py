"""Tenant model."""
import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
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
    api_keys: Mapped[list["ApiKey"]] = relationship(back_populates="tenant", lazy="select")
    destinations: Mapped[list["Destination"]] = relationship(back_populates="tenant", lazy="select")
    events: Mapped[list["Event"]] = relationship(back_populates="tenant", lazy="select")


# Avoid circular import - these are resolved by SQLAlchemy at runtime
from app.models.api_key import ApiKey  # noqa: E402
from app.models.destination import Destination  # noqa: E402
from app.models.event import Event  # noqa: E402
