"""API authentication dependency.

Resolves the API key from the Authorization header, verifies it against
the database, and returns the authenticated tenant.
"""
import uuid
import time
import threading

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.core.security import hash_api_key, verify_api_key, API_KEY_PREFIX_LENGTH
from app.db.session import get_db
from app.models.api_key import ApiKey
from app.models.tenant import Tenant

logger = get_logger(__name__)

security_scheme = HTTPBearer()

_key_cache_lock = threading.Lock()
_key_cache: dict[str, tuple[Tenant, float]] = {}
_CACHE_TTL_SECONDS = 30.0


def get_current_tenant(
    credentials: HTTPAuthorizationCredentials = Security(security_scheme),
    session: Session = Depends(get_db),
) -> Tenant:
    """FastAPI dependency: extract and verify API key, return the owning tenant.

    Lookup strategy: compute the SHA-256 hash of the provided key and match
    it directly against api_keys.key_hash. Caches valid tenant lookups in-memory
    with a short TTL (30s) to eliminate redundant database round-trips under high load.

    Raises:
        HTTPException 401: If the key is missing, invalid, or revoked.
    """
    raw_key = credentials.credentials
    key_hash = hash_api_key(raw_key)

    now = time.time()
    with _key_cache_lock:
        cached = _key_cache.get(key_hash)
        if cached:
            tenant, expires_at = cached
            if now < expires_at:
                return tenant

    # Single joined query to fetch tenant directly
    stmt = (
        select(Tenant)
        .join(ApiKey, ApiKey.tenant_id == Tenant.id)
        .where(ApiKey.key_hash == key_hash, ApiKey.revoked_at.is_(None))
    )
    tenant = session.execute(stmt).scalar_one_or_none()

    if tenant is None:
        check_stmt = select(ApiKey).where(ApiKey.key_hash == key_hash)
        key_record = session.execute(check_stmt).scalar_one_or_none()
        if key_record and not key_record.is_active:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="API key has been revoked",
            )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    # Detach tenant model from session for safe caching across request threads
    session.expunge(tenant)

    with _key_cache_lock:
        _key_cache[key_hash] = (tenant, now + _CACHE_TTL_SECONDS)
        if len(_key_cache) > 2000:
            _key_cache.clear()

    return tenant
