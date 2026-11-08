"""Unit and integration tests for retry policy and backoff behavior."""
import random
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import httpx
import pytest

from app.core.config import get_settings
from app.models.delivery import EventDelivery
from app.models.destination import Destination
from app.models.event import Event
from app.workers.retry_policy import (
    is_retryable_status,
    is_retryable_exception,
    compute_backoff_seconds,
    get_error_category,
    RETRYABLE_STATUS_CODES,
)
from app.workers.tasks import deliver_webhook


def _create_delivery(db_session, tenant, destination, attempt_count=0, status="pending"):
    event = Event(
        tenant_id=tenant.id,
        event_type="test.retry.event",
        payload={"msg": "retry test"},
    )
    db_session.add(event)
    db_session.flush()

    delivery = EventDelivery(
        tenant_id=tenant.id,
        event_id=event.id,
        destination_id=destination.id,
        status=status,
        attempt_count=attempt_count,
    )
    db_session.add(delivery)
    db_session.commit()
    return event, delivery


class TestRetryPolicyUnit:
    """Unit tests for retry classification and math functions."""

    def test_retryable_status_codes(self):
        """All expected status codes are marked retryable; others are not."""
        for code in (408, 429, 500, 502, 503, 504, 522, 524):
            assert is_retryable_status(code) is True, f"{code} should be retryable"
        for code in (200, 201, 204, 400, 401, 403, 404, 422):
            assert is_retryable_status(code) is False, f"{code} should NOT be retryable"

    def test_retryable_exceptions(self):
        """Network and timeout exceptions are retryable; arbitrary exceptions are not."""
        assert is_retryable_exception(httpx.ConnectTimeout("timeout")) is True
        assert is_retryable_exception(httpx.ReadTimeout("timeout")) is True
        assert is_retryable_exception(httpx.ConnectError("refused")) is True
        assert is_retryable_exception(httpx.NetworkError("network")) is True
        assert is_retryable_exception(ValueError("bad value")) is False
        assert is_retryable_exception(KeyError("missing key")) is False

    def test_backoff_increases_exponentially(self):
        """Backoff follows base_delay * 2^(attempt - 1) up to max_delay."""
        base = 2.0
        max_d = 60.0

        # Without jitter: strictly deterministic exponential growth
        d1 = compute_backoff_seconds(1, base, max_d, jitter_enabled=False)
        d2 = compute_backoff_seconds(2, base, max_d, jitter_enabled=False)
        d3 = compute_backoff_seconds(3, base, max_d, jitter_enabled=False)
        d4 = compute_backoff_seconds(4, base, max_d, jitter_enabled=False)
        d7 = compute_backoff_seconds(7, base, max_d, jitter_enabled=False)

        assert d1 == 2.0   # 2 * 2^0
        assert d2 == 4.0   # 2 * 2^1
        assert d3 == 8.0   # 2 * 2^2
        assert d4 == 16.0  # 2 * 2^3
        assert d7 == 60.0  # capped at max_delay

    def test_jitter_within_bounds(self):
        """With jitter enabled, delay is uniformly distributed in [0, max_calculated_delay]."""
        base = 2.0
        max_d = 60.0
        calculated_max = 8.0  # attempt 3 -> 2 * 2^2 = 8

        rng = random.Random(42)
        delays = [
            compute_backoff_seconds(3, base, max_d, jitter_enabled=True, _random=rng)
            for _ in range(100)
        ]

        assert all(0.0 <= d <= calculated_max for d in delays)
        # Jitter produces different values
        assert len(set(delays)) > 80

    def test_error_category_classification(self):
        """Error category strings are bounded and human-readable."""
        assert get_error_category(httpx.ConnectError("fail"), None) == "connect_error"
        assert get_error_category(httpx.ReadTimeout("fail"), None) == "read_timeout"
        assert get_error_category(None, 200) == "success"
        assert get_error_category(None, 429) == "rate_limited_by_destination"
        assert get_error_category(None, 503) == "server_error"
        assert get_error_category(None, 404) == "client_error"


class TestRetryWorkerIntegration:
    """Integration tests for worker retry and dead-letter behavior."""

    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_http_200_succeeds_without_retry(
        self, mock_client_cls, mock_create_session, db_session, demo_tenant
    ):
        """Requirement 1: HTTP 200 succeeds without scheduling a retry."""
        dest = Destination(tenant_id=demo_tenant.id, name="D1", url="http://example.com/ok")
        db_session.add(dest)
        db_session.flush()

        event, delivery = _create_delivery(db_session, demo_tenant, dest)
        mock_create_session.return_value = db_session

        mock_resp = MagicMock(status_code=200)
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        with patch("app.workers.tasks.deliver_webhook.apply_async") as mock_apply:
            res = deliver_webhook(str(delivery.id))
            assert res["status"] == "succeeded"
            mock_apply.assert_not_called()

        db_session.expire_all()
        updated = db_session.get(EventDelivery, delivery.id)
        assert updated.status == "succeeded"
        assert updated.attempt_count == 1

    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_http_500_triggers_retry(
        self, mock_client_cls, mock_create_session, db_session, demo_tenant
    ):
        """Requirement 2: HTTP 500 triggers retry scheduling."""
        dest = Destination(tenant_id=demo_tenant.id, name="D2", url="http://example.com/500")
        db_session.add(dest)
        db_session.flush()

        event, delivery = _create_delivery(db_session, demo_tenant, dest)
        mock_create_session.return_value = db_session

        mock_resp = MagicMock(status_code=500)
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        with patch("app.workers.tasks.deliver_webhook.apply_async") as mock_apply:
            res = deliver_webhook(str(delivery.id))
            assert res["status"] == "retry_scheduled"
            assert res["next_attempt_in_seconds"] > 0
            mock_apply.assert_called_once()

        db_session.expire_all()
        updated = db_session.get(EventDelivery, delivery.id)
        assert updated.status == "retry_scheduled"
        assert updated.next_attempt_at is not None

    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_http_429_follows_retry_policy(
        self, mock_client_cls, mock_create_session, db_session, demo_tenant
    ):
        """Requirement 3: HTTP 429 triggers retry."""
        dest = Destination(tenant_id=demo_tenant.id, name="D3", url="http://example.com/429")
        db_session.add(dest)
        db_session.flush()

        event, delivery = _create_delivery(db_session, demo_tenant, dest)
        mock_create_session.return_value = db_session

        mock_resp = MagicMock(status_code=429)
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        with patch("app.workers.tasks.deliver_webhook.apply_async") as mock_apply:
            res = deliver_webhook(str(delivery.id))
            assert res["status"] == "retry_scheduled"
            mock_apply.assert_called_once()

    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_connection_error_triggers_retry(
        self, mock_client_cls, mock_create_session, db_session, demo_tenant
    ):
        """Requirement 4: Connection error triggers retry."""
        dest = Destination(tenant_id=demo_tenant.id, name="D4", url="http://example.com/conn")
        db_session.add(dest)
        db_session.flush()

        event, delivery = _create_delivery(db_session, demo_tenant, dest)
        mock_create_session.return_value = db_session

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.side_effect = httpx.ConnectError("Refused")
        mock_client_cls.return_value = mock_client

        with patch("app.workers.tasks.deliver_webhook.apply_async") as mock_apply:
            res = deliver_webhook(str(delivery.id))
            assert res["status"] == "retry_scheduled"
            mock_apply.assert_called_once()

    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_timeout_triggers_retry(
        self, mock_client_cls, mock_create_session, db_session, demo_tenant
    ):
        """Requirement 5: Timeout triggers retry."""
        dest = Destination(tenant_id=demo_tenant.id, name="D5", url="http://example.com/timeout")
        db_session.add(dest)
        db_session.flush()

        event, delivery = _create_delivery(db_session, demo_tenant, dest)
        mock_create_session.return_value = db_session

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.side_effect = httpx.ReadTimeout("Timeout")
        mock_client_cls.return_value = mock_client

        with patch("app.workers.tasks.deliver_webhook.apply_async") as mock_apply:
            res = deliver_webhook(str(delivery.id))
            assert res["status"] == "retry_scheduled"
            mock_apply.assert_called_once()

    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_non_retryable_error_does_not_retry(
        self, mock_client_cls, mock_create_session, db_session, demo_tenant
    ):
        """Requirement 6: Non-retryable error (e.g. 400) immediately fails without retries."""
        dest = Destination(tenant_id=demo_tenant.id, name="D6", url="http://example.com/400")
        db_session.add(dest)
        db_session.flush()

        event, delivery = _create_delivery(db_session, demo_tenant, dest)
        mock_create_session.return_value = db_session

        mock_resp = MagicMock(status_code=400)
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        with patch("app.workers.tasks.deliver_webhook.apply_async") as mock_apply:
            res = deliver_webhook(str(delivery.id))
            assert res["status"] == "failed"
            mock_apply.assert_not_called()

        db_session.expire_all()
        updated = db_session.get(EventDelivery, delivery.id)
        assert updated.status == "failed"

    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_max_attempts_exhausted_enters_dead_letter(
        self, mock_client_cls, mock_create_session, db_session, demo_tenant
    ):
        """Requirement 9: When maximum attempts are exhausted, delivery transitions to dead_lettered."""
        dest = Destination(tenant_id=demo_tenant.id, name="D9", url="http://example.com/dead")
        db_session.add(dest)
        db_session.flush()

        # Start with attempt_count = 4 (settings.retry_max_attempts = 5)
        # So this execution is attempt #5 (the last permitted attempt)
        event, delivery = _create_delivery(
            db_session, demo_tenant, dest, attempt_count=4, status="retry_scheduled"
        )
        mock_create_session.return_value = db_session

        mock_resp = MagicMock(status_code=503)
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        with patch("app.workers.tasks.deliver_webhook.apply_async") as mock_apply:
            res = deliver_webhook(str(delivery.id))
            assert res["status"] == "dead_lettered"
            mock_apply.assert_not_called()

        db_session.expire_all()
        updated = db_session.get(EventDelivery, delivery.id)
        assert updated.status == "dead_lettered"
        assert updated.attempt_count == 5
        assert updated.dead_lettered_at is not None

    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_successful_delivery_after_earlier_failure(
        self, mock_client_cls, mock_create_session, db_session, demo_tenant
    ):
        """Requirement 10: Successful retry attempt produces status succeeded."""
        dest = Destination(tenant_id=demo_tenant.id, name="D10", url="http://example.com/retry-ok")
        db_session.add(dest)
        db_session.flush()

        # Started at attempt_count = 1 with status retry_scheduled
        event, delivery = _create_delivery(
            db_session, demo_tenant, dest, attempt_count=1, status="retry_scheduled"
        )
        mock_create_session.return_value = db_session

        mock_resp = MagicMock(status_code=200)
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        with patch("app.workers.tasks.deliver_webhook.apply_async") as mock_apply:
            res = deliver_webhook(str(delivery.id))
            assert res["status"] == "succeeded"
            mock_apply.assert_not_called()

        db_session.expire_all()
        updated = db_session.get(EventDelivery, delivery.id)
        assert updated.status == "succeeded"
        assert updated.attempt_count == 2
        assert updated.last_error is None

    @patch("app.workers.tasks.create_session")
    def test_duplicate_task_after_retry_scheduled_discarded_if_processing(
        self, mock_create_session, db_session, demo_tenant
    ):
        """Requirement 12: Duplicate task is discarded if delivery is already processing."""
        dest = Destination(tenant_id=demo_tenant.id, name="D12", url="http://example.com/dup")
        db_session.add(dest)
        db_session.flush()

        event, delivery = _create_delivery(
            db_session, demo_tenant, dest, attempt_count=1, status="processing"
        )
        mock_create_session.return_value = db_session

        res = deliver_webhook(str(delivery.id))
        assert res["status"] == "discarded"
        assert res["reason"] == "already_processing"

    @patch("app.workers.tasks.create_session")
    def test_dead_lettered_delivery_discarded(
        self, mock_create_session, db_session, demo_tenant
    ):
        """A dead-lettered delivery task is safely discarded."""
        dest = Destination(tenant_id=demo_tenant.id, name="D13", url="http://example.com/dlq")
        db_session.add(dest)
        db_session.flush()

        event, delivery = _create_delivery(
            db_session, demo_tenant, dest, attempt_count=5, status="dead_lettered"
        )
        mock_create_session.return_value = db_session

        res = deliver_webhook(str(delivery.id))
        assert res["status"] == "discarded"
        assert res["reason"] == "dead_lettered"

