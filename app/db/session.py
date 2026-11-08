"""Database session management.

Uses synchronous SQLAlchemy sessions for consistency with synchronous
Celery workers. FastAPI endpoints use sync functions where DB access is needed.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator

from app.core.config import get_settings

_engine = None
_SessionLocal = None


def get_engine():
    """Get or create the SQLAlchemy engine (singleton)."""
    global _engine
    if _engine is None:
        settings = get_settings()
        kwargs = {
            "pool_pre_ping": True,
            "pool_recycle": 300,
        }
        if "sqlite" not in settings.database_url:
            kwargs["pool_size"] = 30
            kwargs["max_overflow"] = 30
            kwargs["pool_timeout"] = 15
        _engine = create_engine(
            settings.database_url,
            **kwargs,
        )
    return _engine


def get_session_factory() -> sessionmaker:
    """Get or create the session factory (singleton)."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            bind=get_engine(),
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
    return _SessionLocal


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency that yields a database session and ensures cleanup."""
    SessionLocal = get_session_factory()
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def create_session() -> Session:
    """Create a new database session (for workers/scripts, not FastAPI)."""
    SessionLocal = get_session_factory()
    return SessionLocal()


def reset_engine() -> None:
    """Reset engine and session factory (for testing)."""
    global _engine, _SessionLocal
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _SessionLocal = None
