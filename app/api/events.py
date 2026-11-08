"""Event API endpoints."""
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.api.auth import get_current_tenant
from app.core.config import get_settings
from app.core.logging import get_logger
from app.db.session import get_db
from app.models.tenant import Tenant
from app.schemas.event import (
    EventCreate,
    EventCreateResponse,
    EventResponse,
    DeliverySummary,
)
from app.core.metrics import EVENTS_RECEIVED_TOTAL
from app.services.event_service import ingest_event, get_event, EventIngestionError

logger = get_logger(__name__)
router = APIRouter(prefix="/v1/events", tags=["events"])


@router.post("", response_model=EventCreateResponse, status_code=status.HTTP_202_ACCEPTED)
def create_event_endpoint(
    body: EventCreate,
    request: Request,
    tenant: Tenant = Depends(get_current_tenant),
    session: Session = Depends(get_db),
):
    """Submit an event for asynchronous webhook delivery.

    The event, delivery records, and outbox entries are created in a single
    PostgreSQL transaction. HTTP 202 means the work is durably accepted,
    NOT that delivery has completed.
    """
    # Enforce payload size limit
    settings = get_settings()
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > settings.max_event_payload_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Request body exceeds maximum size of {settings.max_event_payload_bytes} bytes",
        )

    try:
        event, deliveries = ingest_event(
            session=session,
            tenant_id=tenant.id,
            event_type=body.event_type,
            payload=body.payload,
            destination_ids=body.destination_ids,
        )
        EVENTS_RECEIVED_TOTAL.labels(event_type=body.event_type).inc()
    except EventIngestionError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        )

    return EventCreateResponse(
        event_id=event.id,
        status="accepted",
        delivery_count=len(deliveries),
        deliveries=[
            DeliverySummary(
                delivery_id=d.id,
                destination_id=d.destination_id,
                status=d.status,
            )
            for d in deliveries
        ],
    )


@router.get("/{event_id}", response_model=EventResponse)
def get_event_endpoint(
    event_id: uuid.UUID,
    tenant: Tenant = Depends(get_current_tenant),
    session: Session = Depends(get_db),
):
    """Retrieve an event and its delivery summary."""
    event = get_event(session, event_id, tenant.id)
    if event is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Event not found",
        )

    return EventResponse(
        id=event.id,
        event_type=event.event_type,
        payload=event.payload,
        created_at=event.created_at,
        deliveries=[
            DeliverySummary(
                delivery_id=d.id,
                destination_id=d.destination_id,
                status=d.status,
            )
            for d in event.deliveries
        ],
    )
