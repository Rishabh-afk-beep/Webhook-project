"""Pytest configuration and fixtures.

Uses an in-process SQLite database for unit tests by default.
For integration tests requiring PostgreSQL, set TEST_DATABASE_URL.
"""
import os
import uuid
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool

# Override settings before importing app modules
os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("CELERY_BROKER_URL", "redis://localhost:6379/0")
os.environ.setdefault("CELERY_TASK_ALWAYS_EAGER", "true")
os.environ.setdefault("ALLOW_LOCAL_TEST_DESTINATIONS", "test-receiver,localhost,example.com,testserver")

from app.core.config import reset_settings, get_settings
from app.core.security import generate_api_key
from app.db.base import Base
from app.db.session import get_db, reset_engine
from app.models.tenant import Tenant
from app.models.api_key import ApiKey
from app.models.destination import Destination
from app.models.event import Event
from app.models.delivery import EventDelivery
from app.models.delivery_attempt import DeliveryAttempt
from app.models.outbox import OutboxEntry

# Reset settings to pick up test environment
reset_settings()


@pytest.fixture(scope="session")
def engine():
    """Create a test database engine.

    Uses SQLite in-memory for fast unit tests. The CHECK constraints
    with IN clauses work on SQLite too.
    """
    test_db_url = os.environ.get("TEST_DATABASE_URL", "sqlite:///:memory:")

    if "sqlite" in test_db_url:
        eng = create_engine(
            test_db_url,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        # Enable foreign key enforcement on SQLite
        @event.listens_for(eng, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()
    else:
        eng = create_engine(test_db_url)

    # Create all tables
    Base.metadata.create_all(bind=eng)
    yield eng
    Base.metadata.drop_all(bind=eng)
    eng.dispose()


@pytest.fixture(scope="session")
def TestSessionLocal(engine):
    """Session factory bound to the test engine."""
    return sessionmaker(bind=engine, autocommit=False, autoflush=False, expire_on_commit=False)


@pytest.fixture()
def db_session(engine, TestSessionLocal):
    """Provide a clean database session per test.

    Uses a nested transaction that is rolled back after each test,
    ensuring test isolation without recreating the schema.
    """
    connection = engine.connect()
    transaction = connection.begin()
    session = TestSessionLocal(bind=connection)

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture()
def client(db_session):
    """FastAPI TestClient wired to the test database session."""
    from app.main import app

    def _override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture()
def demo_tenant(db_session) -> Tenant:
    """Create a demo tenant for testing."""
    tenant = Tenant(name="test-tenant")
    db_session.add(tenant)
    db_session.flush()
    return tenant


@pytest.fixture()
def demo_api_key(db_session, demo_tenant) -> tuple[str, ApiKey]:
    """Create a demo API key and return (raw_key, api_key_model)."""
    raw_key, key_prefix, key_hash = generate_api_key()
    api_key = ApiKey(
        tenant_id=demo_tenant.id,
        key_prefix=key_prefix,
        key_hash=key_hash,
    )
    db_session.add(api_key)
    db_session.flush()
    return raw_key, api_key


@pytest.fixture()
def auth_headers(demo_api_key) -> dict[str, str]:
    """Authorization headers for an authenticated request."""
    raw_key, _ = demo_api_key
    return {"Authorization": f"Bearer {raw_key}"}


@pytest.fixture()
def other_tenant(db_session) -> Tenant:
    """Create a second tenant for cross-tenant isolation tests."""
    tenant = Tenant(name="other-tenant")
    db_session.add(tenant)
    db_session.flush()
    return tenant


@pytest.fixture()
def other_api_key(db_session, other_tenant) -> tuple[str, ApiKey]:
    """API key for the other tenant."""
    raw_key, key_prefix, key_hash = generate_api_key()
    api_key = ApiKey(
        tenant_id=other_tenant.id,
        key_prefix=key_prefix,
        key_hash=key_hash,
    )
    db_session.add(api_key)
    db_session.flush()
    return raw_key, api_key


@pytest.fixture()
def other_auth_headers(other_api_key) -> dict[str, str]:
    """Auth headers for the other tenant."""
    raw_key, _ = other_api_key
    return {"Authorization": f"Bearer {raw_key}"}


@pytest.fixture()
def sample_destination(db_session, demo_tenant) -> Destination:
    """Create a sample active destination."""
    dest = Destination(
        tenant_id=demo_tenant.id,
        name="Test Success Endpoint",
        url="http://test-receiver:9000/webhooks/success",
        signing_secret="test-signing-secret-abc123",
        is_active=True,
    )
    db_session.add(dest)
    db_session.flush()
    return dest


@pytest.fixture()
def inactive_destination(db_session, demo_tenant) -> Destination:
    """Create an inactive destination."""
    dest = Destination(
        tenant_id=demo_tenant.id,
        name="Inactive Endpoint",
        url="http://test-receiver:9000/webhooks/success",
        is_active=False,
    )
    db_session.add(dest)
    db_session.flush()
    return dest
