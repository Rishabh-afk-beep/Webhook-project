"""Retry policy: classifies outcomes and computes exponential backoff with jitter."""
import random
from typing import Optional

import httpx

# HTTP status codes that should trigger a retry
RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504, 522, 524})


def is_retryable_status(status_code: int) -> bool:
    """Return True if the HTTP status code should trigger a retry."""
    return status_code in RETRYABLE_STATUS_CODES


def is_retryable_exception(exc: Exception) -> bool:
    """Return True if the exception should trigger a retry."""
    return isinstance(exc, (httpx.TimeoutException, httpx.ConnectError, httpx.NetworkError))


def compute_backoff_seconds(
    attempt_number: int,
    base_delay: float,
    max_delay: float,
    jitter_enabled: bool,
    _random: Optional[random.Random] = None,
) -> float:
    """Compute exponential backoff delay in seconds.

    Formula: min(max_delay, base_delay * 2^(attempt_number - 1))
    With optional uniform jitter in [0, delay].

    Args:
        attempt_number: 1-based number of the attempt that just failed.
        base_delay: base delay in seconds.
        max_delay: maximum delay cap in seconds.
        jitter_enabled: whether to add random jitter.
        _random: optional Random instance for deterministic testing.

    Returns:
        Delay in seconds (float).
    """
    delay = min(max_delay, base_delay * (2 ** (attempt_number - 1)))
    if jitter_enabled:
        rng = _random if _random is not None else random
        delay = rng.uniform(0, delay)
    return delay


def get_error_category(
    exc: Optional[Exception],
    http_status: Optional[int],
) -> str:
    """Return a bounded error category string (max 50 chars).

    This is used for structured logging and metrics, not stored as
    high-cardinality data.
    """
    if exc is not None:
        if isinstance(exc, httpx.ConnectTimeout):
            return "connect_timeout"
        if isinstance(exc, httpx.ReadTimeout):
            return "read_timeout"
        if isinstance(exc, httpx.WriteTimeout):
            return "write_timeout"
        if isinstance(exc, httpx.PoolTimeout):
            return "pool_timeout"
        if isinstance(exc, httpx.TimeoutException):
            return "timeout"
        if isinstance(exc, httpx.ConnectError):
            return "connect_error"
        if isinstance(exc, httpx.NetworkError):
            return "network_error"
        if isinstance(exc, httpx.RemoteProtocolError):
            return "protocol_error"
        return "unexpected_error"
    if http_status is not None:
        if 200 <= http_status < 300:
            return "success"
        if http_status == 429:
            return "rate_limited_by_destination"
        if http_status == 408:
            return "request_timeout"
        if http_status >= 500:
            return "server_error"
        if 400 <= http_status < 500:
            return "client_error"
    return "unknown"
