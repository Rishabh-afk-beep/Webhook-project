"""Tests for destination-specific distributed rate limiting."""
import time
from unittest.mock import MagicMock, patch
import pytest

from app.core.rate_limiter import RateLimiter, get_rate_limiter
from app.models.destination import Destination
from app.models.delivery import EventDelivery
from app.models.event import Event
from app.workers.tasks import deliver_webhook


class DummyRedis:
    """In-memory Redis mock supporting token bucket evaluation for unit tests."""

    def __init__(self):
        self.data = {}
        self.scripts = {}
        self._sha_counter = 0

    def script_load(self, script: str) -> str:
        self._sha_counter += 1
        sha = f"sha_{self._sha_counter}"
        self.scripts[sha] = script
        return sha

    def evalsha(self, sha, numkeys, key, rate, burst, now, requested):
        rate = float(rate)
        burst = float(burst)
        now = float(now)
        requested = float(requested)

        bucket = self.data.get(key, {})
        tokens = bucket.get("tokens")
        last_refill = bucket.get("last_refill")

        if tokens is None or last_refill is None:
            tokens = burst
            last_refill = now
        else:
            elapsed = max(0.0, now - last_refill)
            tokens = min(burst, tokens + (elapsed * rate))
            last_refill = now

        allowed = 0
        retry_after_ms = 0

        if tokens >= requested:
            tokens -= requested
            allowed = 1
        else:
            deficit = requested - tokens
            retry_after_ms = int((deficit / rate) * 1000)
            if retry_after_ms < 50:
                retry_after_ms = 50

        self.data[key] = {"tokens": tokens, "last_refill": last_refill}
        return [allowed, retry_after_ms]


class TestRateLimiter:
    """Unit tests for the distributed rate limiter."""

    def test_requests_under_limit_allowed(self):
        """Requests within burst capacity are allowed immediately."""
        fake_redis = DummyRedis()
        limiter = RateLimiter(redis_client=fake_redis)

        dest_id = "dest-1"
        for _ in range(5):
            allowed, delay = limiter.check_rate_limit(dest_id, rate_per_second=10.0, burst=5)
            assert allowed is True
            assert delay == 0

    def test_requests_exceeding_limit_delayed(self):
        """Exceeding burst capacity returns allowed=False with calculated delay."""
        fake_redis = DummyRedis()
        limiter = RateLimiter(redis_client=fake_redis)

        dest_id = "dest-2"
        # Consume all 2 tokens
        limiter.check_rate_limit(dest_id, rate_per_second=2.0, burst=2)
        limiter.check_rate_limit(dest_id, rate_per_second=2.0, burst=2)

        # 3rd request exceeds limit
        allowed, delay_ms = limiter.check_rate_limit(dest_id, rate_per_second=2.0, burst=2)
        assert allowed is False
        assert delay_ms > 0

    def test_different_destinations_have_independent_limits(self):
        """Rate limits on dest-A do not affect dest-B."""
        fake_redis = DummyRedis()
        limiter = RateLimiter(redis_client=fake_redis)

        # Exhaust dest-A (burst=1)
        allowed_a1, _ = limiter.check_rate_limit("dest-A", rate_per_second=1.0, burst=1)
        allowed_a2, _ = limiter.check_rate_limit("dest-A", rate_per_second=1.0, burst=1)
        assert allowed_a1 is True
        assert allowed_a2 is False

        # dest-B should still be permitted
        allowed_b, _ = limiter.check_rate_limit("dest-B", rate_per_second=1.0, burst=1)
        assert allowed_b is True

    def test_tokens_replenish_over_time(self):
        """Tokens replenish according to rate_per_second."""
        fake_redis = DummyRedis()
        limiter = RateLimiter(redis_client=fake_redis)

        dest_id = "dest-refill"
        # Exhaust burst of 1
        limiter.check_rate_limit(dest_id, rate_per_second=10.0, burst=1)

        # Simulate time moving forward 0.2s (+2 tokens at 10 tokens/sec)
        with patch("time.time", return_value=time.time() + 0.2):
            allowed, _ = limiter.check_rate_limit(dest_id, rate_per_second=10.0, burst=1)
            assert allowed is True

    def test_redis_error_fails_open_safely(self):
        """When Redis encounters a connection error, limiter fails open with warning."""
        mock_redis = MagicMock()
        import redis
        mock_redis.script_load.side_effect = redis.exceptions.ConnectionError("Redis down")

        limiter = RateLimiter(redis_client=mock_redis)
        allowed, delay = limiter.check_rate_limit("dest-err", rate_per_second=10.0, burst=10)
        assert allowed is True
        assert delay == 0


class TestRateLimiterWorkerIntegration:
    """Integration tests verifying worker behavior when destination is rate-limited."""

    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.get_rate_limiter")
    def test_rate_limited_delivery_deferred_without_fake_attempt(
        self, mock_get_limiter, mock_create_session, db_session, demo_tenant
    ):
        """A rate-limited delivery transitions to retry_scheduled without creating a DeliveryAttempt."""
        dest = Destination(
            tenant_id=demo_tenant.id,
            name="Rate Limited Dest",
            url="http://example.com/rl",
            rate_limit_per_second=5.0,
            rate_limit_burst=10,
        )
        db_session.add(dest)
        db_session.flush()

        event = Event(tenant_id=demo_tenant.id, event_type="order.placed", payload={})
        db_session.add(event)
        db_session.flush()

        delivery = EventDelivery(
            tenant_id=demo_tenant.id,
            event_id=event.id,
            destination_id=dest.id,
            status="pending",
        )
        db_session.add(delivery)
        db_session.commit()

        mock_create_session.return_value = db_session

        # Mock rate limiter to return False (rate limited, retry in 3000ms)
        mock_limiter = MagicMock()
        mock_limiter.check_rate_limit.return_value = (False, 3000)
        mock_get_limiter.return_value = mock_limiter

        with patch("app.workers.tasks.deliver_webhook.apply_async") as mock_apply:
            res = deliver_webhook(str(delivery.id))
            assert res["status"] == "retry_scheduled"
            assert res["reason"] == "rate_limited"
            mock_apply.assert_called_once()

        db_session.expire_all()
        updated = db_session.get(EventDelivery, delivery.id)
        assert updated.status == "retry_scheduled"
        assert updated.next_attempt_at is not None

        # Verify NO DeliveryAttempt row was created!
        assert len(updated.attempts) == 0
        assert updated.attempt_count == 0

