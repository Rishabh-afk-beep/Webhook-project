"""Delivery worker tests.

Tests the deliver_webhook Celery task using mocked HTTP requests
since actual HTTP destinations are not available in unit tests.
"""
import json
import uuid
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

from sqlalchemy import select

from app.models.delivery import EventDelivery
from app.models.delivery_attempt import DeliveryAttempt
from app.models.destination import Destination
from app.models.event import Event
from app.models.outbox import OutboxEntry
from app.services.event_service import ingest_event


def _create_test_delivery(db_session, demo_tenant, sample_destination):
    """Helper to create an event+delivery for testing the worker."""
    event, deliveries = ingest_event(
        session=db_session,
        tenant_id=demo_tenant.id,
        event_type="test.worker",
        payload={"order_id": "test-123"},
        destination_ids=[sample_destination.id],
    )
    return event, deliveries[0]


class TestDeliveryWorker:
    """Tests for the deliver_webhook Celery task."""

    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_successful_delivery(
        self, mock_client_cls, mock_create_session, db_session, demo_tenant, sample_destination
    ):
        """A 200 response should mark the delivery as succeeded."""
        event, delivery = _create_test_delivery(db_session, demo_tenant, sample_destination)

        # Mock session to return our test session
        mock_create_session.return_value = db_session

        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        from app.workers.tasks import deliver_webhook
        result = deliver_webhook(str(delivery.id))

        assert result["status"] == "succeeded"
        db_session.expire_all()
        updated = db_session.get(EventDelivery, delivery.id)
        assert updated.status == "succeeded"
        assert updated.attempt_count == 1

    @patch("app.workers.tasks.deliver_webhook.apply_async")
    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_failed_delivery_500(
        self, mock_client_cls, mock_create_session, mock_apply_async, db_session, demo_tenant, sample_destination
    ):
        """A 500 response should schedule a retry with exponential backoff."""
        event, delivery = _create_test_delivery(db_session, demo_tenant, sample_destination)
        mock_create_session.return_value = db_session

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        from app.workers.tasks import deliver_webhook
        result = deliver_webhook(str(delivery.id))

        assert result["status"] == "retry_scheduled"
        assert result["http_status"] == 500
        db_session.expire_all()
        updated = db_session.get(EventDelivery, delivery.id)
        assert updated.status == "retry_scheduled"
        assert updated.last_http_status == 500
        assert updated.next_attempt_at is not None
        mock_apply_async.assert_called_once()

    @patch("app.workers.tasks.deliver_webhook.apply_async")
    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_failed_delivery_429(
        self, mock_client_cls, mock_create_session, mock_apply_async, db_session, demo_tenant, sample_destination
    ):
        """A 429 response should schedule a retry."""
        event, delivery = _create_test_delivery(db_session, demo_tenant, sample_destination)
        mock_create_session.return_value = db_session

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        from app.workers.tasks import deliver_webhook
        result = deliver_webhook(str(delivery.id))

        assert result["status"] == "retry_scheduled"
        assert result["http_status"] == 429
        mock_apply_async.assert_called_once()

    @patch("app.workers.tasks.deliver_webhook.apply_async")
    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_connection_failure(
        self, mock_client_cls, mock_create_session, mock_apply_async, db_session, demo_tenant, sample_destination
    ):
        """A connection error should schedule a retry with last_error recorded."""
        import httpx
        event, delivery = _create_test_delivery(db_session, demo_tenant, sample_destination)
        mock_create_session.return_value = db_session

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.side_effect = httpx.ConnectError("Connection refused")
        mock_client_cls.return_value = mock_client

        from app.workers.tasks import deliver_webhook
        result = deliver_webhook(str(delivery.id))

        assert result["status"] == "retry_scheduled"
        db_session.expire_all()
        updated = db_session.get(EventDelivery, delivery.id)
        assert updated.status == "retry_scheduled"
        assert updated.last_error == "Connection failed"
        mock_apply_async.assert_called_once()

    @patch("app.workers.tasks.deliver_webhook.apply_async")
    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_timeout(
        self, mock_client_cls, mock_create_session, mock_apply_async, db_session, demo_tenant, sample_destination
    ):
        """A timeout should schedule a retry."""
        import httpx
        event, delivery = _create_test_delivery(db_session, demo_tenant, sample_destination)
        mock_create_session.return_value = db_session

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.side_effect = httpx.ReadTimeout("Read timed out")
        mock_client_cls.return_value = mock_client

        from app.workers.tasks import deliver_webhook
        result = deliver_webhook(str(delivery.id))

        assert result["status"] == "retry_scheduled"
        db_session.expire_all()
        updated = db_session.get(EventDelivery, delivery.id)
        assert updated.last_error == "Request timed out"
        mock_apply_async.assert_called_once()

    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_non_retryable_failure_400(
        self, mock_client_cls, mock_create_session, db_session, demo_tenant, sample_destination
    ):
        """A 400 response is non-retryable and should immediately mark the delivery as failed."""
        event, delivery = _create_test_delivery(db_session, demo_tenant, sample_destination)
        mock_create_session.return_value = db_session

        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        from app.workers.tasks import deliver_webhook
        result = deliver_webhook(str(delivery.id))

        assert result["status"] == "failed"
        assert result["http_status"] == 400
        db_session.expire_all()
        updated = db_session.get(EventDelivery, delivery.id)
        assert updated.status == "failed"
        assert updated.last_http_status == 400

    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_attempt_history_recorded(
        self, mock_client_cls, mock_create_session, db_session, demo_tenant, sample_destination
    ):
        """A delivery attempt must create a DeliveryAttempt record."""
        event, delivery = _create_test_delivery(db_session, demo_tenant, sample_destination)
        mock_create_session.return_value = db_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        from app.workers.tasks import deliver_webhook
        deliver_webhook(str(delivery.id))

        attempts = list(db_session.execute(
            select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == delivery.id)
        ).scalars().all())

        assert len(attempts) == 1
        assert attempts[0].attempt_number == 1
        assert attempts[0].outcome == "succeeded"
        assert attempts[0].http_status == 200
        assert attempts[0].response_time_ms is not None

    @patch("app.workers.tasks.create_session")
    def test_duplicate_task_already_succeeded(
        self, mock_create_session, db_session, demo_tenant, sample_destination
    ):
        """A duplicate task for an already-succeeded delivery should be discarded."""
        event, delivery = _create_test_delivery(db_session, demo_tenant, sample_destination)
        delivery.status = "succeeded"
        db_session.flush()

        mock_create_session.return_value = db_session

        from app.workers.tasks import deliver_webhook
        result = deliver_webhook(str(delivery.id))

        assert result["status"] == "discarded"
        assert result["reason"] == "already_succeeded"

    @patch("app.workers.tasks.create_session")
    def test_nonexistent_delivery(self, mock_create_session, db_session):
        """A task for a nonexistent delivery should be discarded."""
        mock_create_session.return_value = db_session

        from app.workers.tasks import deliver_webhook
        result = deliver_webhook(str(uuid.uuid4()))

        assert result["status"] == "discarded"
        assert result["reason"] == "delivery_not_found"

    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_inactive_destination_handling(
        self, mock_client_cls, mock_create_session, db_session, demo_tenant, sample_destination
    ):
        """Delivery to a destination that became inactive should fail without making HTTP request."""
        event, delivery = _create_test_delivery(db_session, demo_tenant, sample_destination)
        # Deactivate destination after ingestion
        sample_destination.is_active = False
        db_session.flush()

        mock_create_session.return_value = db_session

        from app.workers.tasks import deliver_webhook
        result = deliver_webhook(str(delivery.id))

        assert result["status"] == "failed"
        assert result["reason"] == "destination_inactive"
        # HTTP client should NOT have been called
        mock_client_cls.assert_not_called()

    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.httpx.Client")
    def test_signing_headers_present(
        self, mock_client_cls, mock_create_session, db_session, demo_tenant, sample_destination
    ):
        """When a signing secret is configured, the signature header should be included."""
        event, delivery = _create_test_delivery(db_session, demo_tenant, sample_destination)
        mock_create_session.return_value = db_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        from app.workers.tasks import deliver_webhook
        deliver_webhook(str(delivery.id))

        # Check that post was called with signature header
        call_args = mock_client.post.call_args
        headers = call_args.kwargs.get("headers", {})
        assert "X-Webhook-Signature" in headers
        assert headers["X-Webhook-Signature"].startswith("sha256=")
