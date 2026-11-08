"""Delivery API endpoints."""
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.auth import get_current_tenant
from app.db.session import get_db
from app.models.tenant import Tenant
from app.schemas.delivery import (
    DeliveryResponse,
    DeliveryAttemptResponse,
    DeliveryAttemptListResponse,
)
from app.services.delivery_service import get_delivery, get_delivery_attempts

router = APIRouter(prefix="/v1/deliveries", tags=["deliveries"])


@router.get("/{delivery_id}", response_model=DeliveryResponse)
def get_delivery_endpoint(
    delivery_id: uuid.UUID,
    tenant: Tenant = Depends(get_current_tenant),
    session: Session = Depends(get_db),
):
    """Get delivery status for a specific delivery."""
    delivery = get_delivery(session, delivery_id, tenant.id)
    if delivery is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Delivery not found",
        )
    return delivery


@router.get("/{delivery_id}/attempts", response_model=DeliveryAttemptListResponse)
def get_delivery_attempts_endpoint(
    delivery_id: uuid.UUID,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    tenant: Tenant = Depends(get_current_tenant),
    session: Session = Depends(get_db),
):
    """Get the attempt history for a delivery in chronological order."""
    # Verify ownership first
    delivery = get_delivery(session, delivery_id, tenant.id)
    if delivery is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Delivery not found",
        )

    attempts, total = get_delivery_attempts(
        session=session,
        delivery_id=delivery_id,
        tenant_id=tenant.id,
        offset=offset,
        limit=limit,
    )
    return DeliveryAttemptListResponse(items=attempts, total=total)
