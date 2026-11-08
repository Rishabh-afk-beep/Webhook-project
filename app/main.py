"""FastAPI application entry point."""
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.core.config import get_settings
from app.core.logging import setup_logging, get_logger
from app.api.health import router as health_router
from app.api.destinations import router as destinations_router
from app.api.events import router as events_router
from app.api.deliveries import router as deliveries_router
from app.api.dead_letters import router as dead_letters_router
from app.api.metrics import router as metrics_router

setup_logging()
logger = get_logger(__name__)


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title="Reliable Webhook Delivery Platform",
        description="Accepts events from authenticated clients and delivers them to configured HTTP endpoints.",
        version="0.1.0",
    )

    # Register routers
    app.include_router(health_router)
    app.include_router(destinations_router)
    app.include_router(events_router)
    app.include_router(deliveries_router)
    app.include_router(dead_letters_router)
    app.include_router(metrics_router)

    # Global exception handler — never expose internal tracebacks
    @app.exception_handler(Exception)
    def global_exception_handler(request: Request, exc: Exception):
        logger.error(
            "Unhandled exception",
            extra={"path": request.url.path, "error": str(exc)},
            exc_info=True,
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"},
        )

    logger.info("Application created", extra={"app_env": settings.app_env})
    return app


app = create_app()
