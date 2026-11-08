"""Distributed token-bucket rate limiter backed by Redis Lua scripting.

Why Token Bucket?
1. Burst tolerance: Allows bursts up to bucket capacity while enforcing steady replenishment rate.
2. Smooth rate limiting: Refills smoothly based on exact elapsed milliseconds, preventing
   boundary burst issues present in fixed-window algorithms.
3. Atomic multi-worker execution: Redis Lua script executes atomically, ensuring multiple Celery
   workers coordinate without race conditions.
4. Calculated retry delay: When the rate limit is exceeded, the algorithm computes the exact
   delay required for token replenishment, avoiding busy-waiting and rapid polling loops.
"""
import time
from typing import Optional, Tuple
import redis

from app.core.config import get_settings
from app.core.logging import get_logger
from app.core.redis_client import get_redis_client

logger = get_logger(__name__)

# Atomic Token Bucket Lua script
TOKEN_BUCKET_SCRIPT = """
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1])
local last_refill = tonumber(bucket[2])

if tokens == nil or last_refill == nil then
    tokens = burst
    last_refill = now
else
    local elapsed = math.max(0, now - last_refill)
    tokens = math.min(burst, tokens + (elapsed * rate))
    last_refill = now
end

local allowed = 0
local retry_after_ms = 0

if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
else
    local deficit = requested - tokens
    retry_after_ms = math.ceil((deficit / rate) * 1000)
    if retry_after_ms < 50 then
        retry_after_ms = 50
    end
end

redis.call('HMSET', key, 'tokens', tokens, 'last_refill', last_refill)
local ttl_sec = math.ceil(burst / rate) + 10
redis.call('EXPIRE', key, ttl_sec)

return {allowed, retry_after_ms}
"""


class RateLimiter:
    """Distributed rate limiter for destination endpoints."""

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self._redis = redis_client
        self._script_sha: Optional[str] = None

    @property
    def redis(self) -> redis.Redis:
        if self._redis is None:
            self._redis = get_redis_client()
        return self._redis

    def _ensure_script(self) -> str:
        """Register the Lua script with Redis if not already registered."""
        if self._script_sha is None:
            self._script_sha = self.redis.script_load(TOKEN_BUCKET_SCRIPT)
        return self._script_sha

    def check_rate_limit(
        self,
        destination_id: str,
        rate_per_second: float,
        burst: int,
    ) -> Tuple[bool, int]:
        """Check if a delivery request is allowed under the destination's rate limit.

        Args:
            destination_id: Unique destination identifier.
            rate_per_second: Sustained token generation rate.
            burst: Maximum bucket capacity.

        Returns:
            Tuple of (allowed: bool, retry_after_ms: int).
            If allowed is True, retry_after_ms is 0.
            If allowed is False, retry_after_ms is the estimated milliseconds to wait.
        """
        key = f"rate_limit:dest:{destination_id}"
        now = time.time()

        try:
            sha = self._ensure_script()
            try:
                res = self.redis.evalsha(sha, 1, key, rate_per_second, burst, now, 1)
            except redis.exceptions.NoScriptError:
                self._script_sha = None
                sha = self._ensure_script()
                res = self.redis.evalsha(sha, 1, key, rate_per_second, burst, now, 1)

            allowed = bool(res[0])
            retry_after_ms = int(res[1])

            if not allowed:
                logger.info(
                    "Destination rate limited",
                    extra={
                        "destination_id": destination_id,
                        "retry_after_ms": retry_after_ms,
                        "rate_per_second": rate_per_second,
                        "burst": burst,
                    },
                )
            return allowed, retry_after_ms

        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            settings = get_settings()
            # Explicit failure policy: fail-open or fail-closed
            # Default is fail-open with clear observable log warning
            if getattr(settings, "rate_limit_fail_closed", False):
                logger.error(
                    "Redis unavailable for rate limiting - deferring delivery (fail-closed policy)",
                    extra={"destination_id": destination_id, "error": str(e)},
                )
                return False, 5000  # Defer for 5 seconds
            else:
                logger.warning(
                    "Redis unavailable for rate limiting - allowing delivery (fail-open policy)",
                    extra={"destination_id": destination_id, "error": str(e)},
                )
                return True, 0


# Singleton instance
_rate_limiter: Optional[RateLimiter] = None


def get_rate_limiter() -> RateLimiter:
    """Return the shared RateLimiter singleton."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = RateLimiter()
    return _rate_limiter

