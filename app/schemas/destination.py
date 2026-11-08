"""Destination Pydantic schemas."""
import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class DestinationCreate(BaseModel):
    """Request schema for creating a destination."""
    name: str = Field(..., min_length=1, max_length=255, description="Human-readable destination name")
    url: str = Field(..., min_length=1, max_length=2048, description="Webhook delivery URL (http or https)")

    @field_validator("url")
    @classmethod
    def url_must_have_scheme(cls, v: str) -> str:
        v = v.strip()
        if not v.startswith(("http://", "https://")):
            raise ValueError("URL must start with http:// or https://")
        return v


class DestinationResponse(BaseModel):
    """Response schema for a destination."""
    id: uuid.UUID
    name: str
    url: str
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class DestinationListResponse(BaseModel):
    """Paginated list of destinations."""
    items: list[DestinationResponse]
    total: int
    offset: int
    limit: int
