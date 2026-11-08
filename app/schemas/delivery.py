"""Delivery and attempt Pydantic schemas."""
import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class DeliveryResponse(BaseModel):
    """Detailed delivery status response."""
    id: uuid.UUID
    event_id: uuid.UUID
    destination_id: uuid.UUID
    status: str
    attempt_count: int
    last_http_status: Optional[int] = None
    last_error: Optional[str] = None
    next_attempt_at: Optional[datetime] = None
    dead_lettered_at: Optional[datetime] = None
    replayed_at: Optional[datetime] = None
    replay_of_delivery_id: Optional[uuid.UUID] = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class DeliveryAttemptResponse(BaseModel):
    """Single delivery attempt record."""
    id: uuid.UUID
    attempt_number: int
    outcome: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    http_status: Optional[int] = None
    response_time_ms: Optional[int] = None
    error_message: Optional[str] = None
    error_category: Optional[str] = None

    model_config = {"from_attributes": True}


class DeliveryAttemptListResponse(BaseModel):
    """List of delivery attempts."""
    items: list[DeliveryAttemptResponse]
    total: int


class DLQItemResponse(BaseModel):
    """Dead-letter queue item with full attempt history."""
    id: uuid.UUID
    event_id: uuid.UUID
    destination_id: uuid.UUID
    status: str
    attempt_count: int
    last_http_status: Optional[int] = None
    last_error: Optional[str] = None
    dead_lettered_at: Optional[datetime] = None
    replayed_at: Optional[datetime] = None
    replay_of_delivery_id: Optional[uuid.UUID] = None
    created_at: datetime
    updated_at: datetime
    attempts: list[DeliveryAttemptResponse] = []

    model_config = {"from_attributes": True}


class DLQListResponse(BaseModel):
    """Paginated list of dead-lettered deliveries."""
    items: list[DLQItemResponse]
    total: int
    limit: int
    offset: int


class ReplayResponse(BaseModel):
    """Response after requesting a delivery replay."""
    original_delivery_id: uuid.UUID
    new_delivery_id: uuid.UUID
    status: str
    message: str
