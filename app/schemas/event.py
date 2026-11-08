"""Event Pydantic schemas."""
import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class EventCreate(BaseModel):
    """Request schema for submitting an event."""
    event_type: str = Field(
        ..., min_length=1, max_length=255,
        description="Event type identifier, e.g. 'order.created'"
    )
    payload: dict[str, Any] = Field(
        ..., description="JSON object payload for the event"
    )
    destination_ids: list[uuid.UUID] = Field(
        ..., min_length=1,
        description="List of destination IDs to deliver the event to (at least one)"
    )

    @field_validator("payload")
    @classmethod
    def payload_must_be_object(cls, v: Any) -> dict:
        if not isinstance(v, dict):
            raise ValueError("Payload must be a JSON object")
        return v

    @field_validator("destination_ids")
    @classmethod
    def destination_ids_must_not_be_empty(cls, v: list[uuid.UUID]) -> list[uuid.UUID]:
        if len(v) == 0:
            raise ValueError("At least one destination_id is required")
        # Remove duplicates while preserving order
        seen = set()
        unique = []
        for did in v:
            if did not in seen:
                seen.add(did)
                unique.append(did)
        return unique


class DeliverySummary(BaseModel):
    """Brief delivery info returned in event creation response."""
    delivery_id: uuid.UUID
    destination_id: uuid.UUID
    status: str

    model_config = {"from_attributes": True}


class EventCreateResponse(BaseModel):
    """Response after successfully accepting an event."""
    event_id: uuid.UUID
    status: str = "accepted"
    delivery_count: int
    deliveries: list[DeliverySummary]


class EventResponse(BaseModel):
    """Detailed event response."""
    id: uuid.UUID
    event_type: str
    payload: dict[str, Any]
    created_at: datetime
    deliveries: list[DeliverySummary] = []

    model_config = {"from_attributes": True}
