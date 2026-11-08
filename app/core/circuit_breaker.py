"""Distributed Circuit Breaker backed by Redis.

States:
- CLOSED: Normal operation. Requests are allowed. Failures are tracked within an observation window.
- OPEN: Failure threshold exceeded. Requests are blocked and deferred without sending HTTP requests.
- HALF_OPEN: Open duration has elapsed. A limited number of probe requests are allowed to test recovery.

Design:
- Uses Redis for shared state across concurrent Celery worker processes.
- Atomic state transitions.
- Defers requests during OPEN/HALF_OPEN saturation without recording fake HTTP attempts.
- Explicit failure policy: fail-open with structured logging on Redis unavailability.
"""
from enum import Enum
import time
from typing import Optional, Tuple
import redis

from app.core.config import get_settings
from app.core.logging import get_logger
from app.core.redis_client import get_redis_client

logger = get_logger(__name__)


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Distributed circuit breaker for destination endpoints."""

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self._redis = redis_client

    @property
    def redis(self) -> redis.Redis:
        if self._redis is None:
            self._redis = get_redis_client()
        return self._redis

    def _keys(self, destination_id: str) -> dict[str, str]:
        prefix = f"circuit_breaker:dest:{destination_id}"
        return {
            "state": f"{prefix}:state",
            "opened_at": f"{prefix}:opened_at",
            "failures": f"{prefix}:failures",
            "probes": f"{prefix}:probes",
            "successes": f"{prefix}:successes",
        }

    def can_execute(self, destination_id: str) -> Tuple[bool, CircuitState, int]:
        """Determine if a request to the destination is permitted.

        Returns:
            Tuple of (allowed: bool, state: CircuitState, retry_after_seconds: int).
        """
        keys = self._keys(destination_id)
        settings = get_settings()
        now = time.time()

        try:
            raw_state = self.redis.get(keys["state"])
            state = CircuitState(raw_state.decode("utf-8")) if raw_state else CircuitState.CLOSED

            if state == CircuitState.CLOSED:
                return True, CircuitState.CLOSED, 0

            if state == CircuitState.OPEN:
                opened_at_val = self.redis.get(keys["opened_at"])
                opened_at = float(opened_at_val) if opened_at_val else now
                elapsed = now - opened_at

                if elapsed >= settings.circuit_breaker_open_duration_seconds:
                    # Transition to HALF_OPEN
                    pipe = self.redis.pipeline()
                    pipe.set(keys["state"], CircuitState.HALF_OPEN.value)
                    pipe.set(keys["probes"], 1)  # current probe
                    pipe.set(keys["successes"], 0)
                    pipe.execute()
                    logger.info(
                        "Circuit breaker transitioned to HALF_OPEN",
                        extra={"destination_id": destination_id},
                    )
                    return True, CircuitState.HALF_OPEN, 0
                else:
                    remaining = int(settings.circuit_breaker_open_duration_seconds - elapsed) + 1
                    return False, CircuitState.OPEN, max(1, remaining)

            if state == CircuitState.HALF_OPEN:
                # Check probe limit
                probes = self.redis.incr(keys["probes"])
                if probes <= settings.circuit_breaker_half_open_probes:
                    return True, CircuitState.HALF_OPEN, 0
                else:
                    # Probe quota full; defer
                    return False, CircuitState.HALF_OPEN, 5

            return True, CircuitState.CLOSED, 0

        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            logger.warning(
                "Redis unavailable for circuit breaker - failing open",
                extra={"destination_id": destination_id, "error": str(e)},
            )
            return True, CircuitState.CLOSED, 0

    def record_success(self, destination_id: str) -> None:
        """Record a successful delivery."""
        keys = self._keys(destination_id)
        settings = get_settings()

        try:
            raw_state = self.redis.get(keys["state"])
            state = CircuitState(raw_state.decode("utf-8")) if raw_state else CircuitState.CLOSED

            if state == CircuitState.HALF_OPEN:
                successes = self.redis.incr(keys["successes"])
                if successes >= settings.circuit_breaker_success_threshold:
                    # Recovery criteria met: close circuit
                    pipe = self.redis.pipeline()
                    pipe.set(keys["state"], CircuitState.CLOSED.value)
                    pipe.delete(keys["failures"])
                    pipe.delete(keys["probes"])
                    pipe.delete(keys["successes"])
                    pipe.delete(keys["opened_at"])
                    pipe.execute()
                    logger.info(
                        "Circuit breaker recovery complete — closed",
                        extra={"destination_id": destination_id},
                    )
            elif state == CircuitState.CLOSED:
                # Optionally clear old failures on success
                pass

        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            logger.warning(
                "Redis error recording circuit breaker success",
                extra={"destination_id": destination_id, "error": str(e)},
            )

    def record_failure(self, destination_id: str, is_qualifying_failure: bool = True) -> None:
        """Record a delivery failure.

        Args:
            destination_id: Unique destination id.
            is_qualifying_failure: True for 5xx, timeouts, connection errors. False for 4xx or rate limit.
        """
        if not is_qualifying_failure:
            return

        keys = self._keys(destination_id)
        settings = get_settings()
        now = time.time()

        try:
            raw_state = self.redis.get(keys["state"])
            state = CircuitState(raw_state.decode("utf-8")) if raw_state else CircuitState.CLOSED

            if state == CircuitState.HALF_OPEN:
                # Probe failed: immediately reopen
                self._open_circuit(destination_id, now)
                logger.warning(
                    "Circuit breaker probe failed — reopening circuit",
                    extra={"destination_id": destination_id},
                )
                return

            if state == CircuitState.CLOSED:
                # Add failure timestamp to sorted set
                cutoff = now - settings.circuit_breaker_observation_window_seconds
                pipe = self.redis.pipeline()
                pipe.zremrangebyscore(keys["failures"], 0, cutoff)
                pipe.zadd(keys["failures"], {str(now): now})
                pipe.zcard(keys["failures"])
                pipe.expire(keys["failures"], settings.circuit_breaker_observation_window_seconds * 2)
                results = pipe.execute()

                failure_count = results[2]
                if failure_count >= settings.circuit_breaker_failure_threshold:
                    self._open_circuit(destination_id, now)
                    logger.warning(
                        "Circuit breaker opened due to repeated failures",
                        extra={
                            "destination_id": destination_id,
                            "failure_count": failure_count,
                            "threshold": settings.circuit_breaker_failure_threshold,
                        },
                    )

        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            logger.warning(
                "Redis error recording circuit breaker failure",
                extra={"destination_id": destination_id, "error": str(e)},
            )

    def _open_circuit(self, destination_id: str, now: float) -> None:
        keys = self._keys(destination_id)
        pipe = self.redis.pipeline()
        pipe.set(keys["state"], CircuitState.OPEN.value)
        pipe.set(keys["opened_at"], now)
        pipe.delete(keys["probes"])
        pipe.delete(keys["successes"])
        pipe.execute()

    def get_state(self, destination_id: str) -> CircuitState:
        """Get the current circuit state for a destination."""
        keys = self._keys(destination_id)
        try:
            raw = self.redis.get(keys["state"])
            return CircuitState(raw.decode("utf-8")) if raw else CircuitState.CLOSED
        except Exception:
            return CircuitState.CLOSED


# Singleton instance
_circuit_breaker: Optional[CircuitBreaker] = None


def get_circuit_breaker() -> CircuitBreaker:
    """Return the shared CircuitBreaker singleton."""
    global _circuit_breaker
    if _circuit_breaker is None:
        _circuit_breaker = CircuitBreaker()
    return _circuit_breaker

