"""Common Pydantic schemas."""
import uuid
from datetime import datetime
from pydantic import BaseModel


class ErrorResponse(BaseModel):
    """Standard error response."""
    detail: str


class PaginationParams(BaseModel):
    """Pagination query parameters."""
    offset: int = 0
    limit: int = 50
