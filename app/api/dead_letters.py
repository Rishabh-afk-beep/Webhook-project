"""Dead-letter queue API endpoints."""
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.auth import get_current_tenant
from app.db.session import get_db
from app.models.tenant import Tenant
from app.schemas.delivery import DLQListResponse, DLQItemResponse, ReplayResponse
from app.services.dlq_service import (
    list_dead_letters,
    get_dead_letter,
    replay_delivery,
    DLQNotFoundError,
    DLQNotDeadLetteredError,
    DLQAlreadyReplayedError,
)

router = APIRouter(prefix="/v1/dead-letters", tags=["dead-letters"])


@router.get("", response_model=DLQListResponse)
def list_dead_letters_endpoint(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    tenant: Tenant = Depends(get_current_tenant),
    session: Session = Depends(get_db),
):
    """List dead-lettered deliveries for the authenticated tenant."""
    deliveries, total = list_dead_letters(
        session=session,
        tenant_id=tenant.id,
        limit=limit,
        offset=offset,
    )
    items = [DLQItemResponse.model_validate(d) for d in deliveries]
    return DLQListResponse(items=items, total=total, limit=limit, offset=offset)


@router.get("/{delivery_id}", response_model=DLQItemResponse)
def get_dead_letter_endpoint(
    delivery_id: uuid.UUID,
    tenant: Tenant = Depends(get_current_tenant),
    session: Session = Depends(get_db),
):
    """Get a specific dead-lettered delivery with full attempt history."""
    try:
        delivery = get_dead_letter(
            session=session,
            tenant_id=tenant.id,
            delivery_id=delivery_id,
        )
    except DLQNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dead-lettered delivery not found",
        )
    except DLQNotDeadLetteredError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dead-lettered delivery not found",
        )
    return DLQItemResponse.model_validate(delivery)


@router.post("/{delivery_id}/replay", status_code=status.HTTP_202_ACCEPTED, response_model=ReplayResponse)
def replay_dead_letter_endpoint(
    delivery_id: uuid.UUID,
    tenant: Tenant = Depends(get_current_tenant),
    session: Session = Depends(get_db),
):
    """Request manual replay of a dead-lettered delivery.

    Creates a NEW EventDelivery linked to the same event and destination.
    The original delivery and its attempt history are preserved unchanged.
    Idempotent: if already replayed (replayed_at is set), returns 409.
    """
    try:
        new_delivery = replay_delivery(
            session=session,
            tenant_id=tenant.id,
            delivery_id=delivery_id,
        )
    except DLQNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dead-lettered delivery not found",
        )
    except DLQNotDeadLetteredError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        )
    except DLQAlreadyReplayedError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Delivery has already been replayed",
        )

    return ReplayResponse(
        original_delivery_id=delivery_id,
        new_delivery_id=new_delivery.id,
        status="pending",
        message="Replay delivery created and queued for delivery",
    )
