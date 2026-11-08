"""Destination API endpoints."""
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.auth import get_current_tenant
from app.db.session import get_db
from app.models.tenant import Tenant
from app.schemas.destination import (
    DestinationCreate,
    DestinationResponse,
    DestinationListResponse,
)
from app.services.destination_service import (
    create_destination,
    list_destinations,
)

router = APIRouter(prefix="/v1/destinations", tags=["destinations"])


@router.post("", response_model=DestinationResponse, status_code=status.HTTP_201_CREATED)
def create_destination_endpoint(
    body: DestinationCreate,
    tenant: Tenant = Depends(get_current_tenant),
    session: Session = Depends(get_db),
):
    """Create a new webhook destination.

    The server generates a signing secret automatically. The signing secret
    is stored securely and is never returned in API responses.
    """
    try:
        destination = create_destination(
            session=session,
            tenant_id=tenant.id,
            name=body.name,
            url=body.url,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        )
    return destination


@router.get("", response_model=DestinationListResponse)
def list_destinations_endpoint(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    tenant: Tenant = Depends(get_current_tenant),
    session: Session = Depends(get_db),
):
    """List destinations belonging to the authenticated tenant."""
    destinations, total = list_destinations(
        session=session,
        tenant_id=tenant.id,
        offset=offset,
        limit=limit,
    )
    return DestinationListResponse(
        items=destinations,
        total=total,
        offset=offset,
        limit=limit,
    )
