"""Health check endpoints."""
from fastapi import APIRouter, status
from sqlalchemy import text

from app.core.logging import get_logger
from app.db.session import get_engine

logger = get_logger(__name__)
router = APIRouter(tags=["health"])


@router.get("/health")
def health_check():
    """Basic liveness check — confirms the API process is responding."""
    return {"status": "ok"}


@router.get("/health/ready")
def readiness_check():
    """Readiness check — verifies the API can reach its required database.

    Returns HTTP 503 if the database connection fails.
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ready", "database": "connected"}
    except Exception as e:
        logger.warning("Readiness check failed", extra={"error": str(e)})
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "not ready", "database": "unavailable"},
        )
