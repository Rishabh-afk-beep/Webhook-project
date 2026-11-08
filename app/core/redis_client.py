"""Redis connection management for distributed rate limiting, circuit breaker, and state."""
import redis
from typing import Optional

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_redis_pool: Optional[redis.ConnectionPool] = None


def get_redis_client() -> redis.Redis:
    """Return a Redis client instance from the shared connection pool."""
    global _redis_pool
    if _redis_pool is None:
        settings = get_settings()
        _redis_pool = redis.ConnectionPool.from_url(
            settings.redis_url,
            decode_responses=False,
            max_connections=20,
            socket_connect_timeout=2.0,
            socket_timeout=2.0,
        )
    return redis.Redis(connection_pool=_redis_pool)


def close_redis_pool() -> None:
    """Close the shared Redis connection pool."""
    global _redis_pool
    if _redis_pool is not None:
        _redis_pool.disconnect()
        _redis_pool = None

