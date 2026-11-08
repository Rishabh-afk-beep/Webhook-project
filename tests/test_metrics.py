"""Tests for Prometheus metrics collection and /metrics endpoint."""
import pytest
from fastapi.testclient import TestClient

from app.core.metrics import (
    EVENTS_RECEIVED_TOTAL,
    DELIVERY_ATTEMPTS_TOTAL,
    DELIVERIES_TOTAL,
    DELIVERY_RETRIES_TOTAL,
    DEAD_LETTERS_TOTAL,
    RATE_LIMIT_DEFERRED_TOTAL,
    CIRCUIT_BREAKER_DEFERRED_TOTAL,
)


class TestMetrics:
    """Verify Prometheus metrics endpoint and instrumentation."""

    def test_metrics_endpoint_accessible(self, client: TestClient):
        """GET /metrics responds with 200 and Prometheus exposition format."""
        response = client.get("/metrics")
        assert response.status_code == 200
        assert "text/plain" in response.headers["content-type"]
        body = response.text
        assert "webhook_events_received_total" in body
        assert "webhook_delivery_attempts_total" in body
        assert "webhook_delivery_duration_seconds" in body

    def test_events_received_metric_increments(self, client: TestClient, auth_headers: dict, db_session, demo_tenant):
        """POST /v1/events increments the webhook_events_received_total counter."""
        from app.models.destination import Destination
        dest = Destination(tenant_id=demo_tenant.id, name="Metric Dest", url="http://example.com/metric")
        db_session.add(dest)
        db_session.commit()

        initial_res = client.get("/metrics")
        initial_body = initial_res.text

        # Ingest an event with specific event_type
        payload = {
            "event_type": "invoice.generated.test_metric",
            "payload": {"inv_id": "123"},
            "destination_ids": [str(dest.id)],
        }
        res = client.post("/v1/events", headers=auth_headers, json=payload)
        assert res.status_code == 202

        metrics_res = client.get("/metrics")
        metrics_body = metrics_res.text
        assert 'webhook_events_received_total{event_type="invoice.generated.test_metric"}' in metrics_body

    def test_no_sensitive_labels_in_metrics(self, client: TestClient):
        """Verify no sensitive attributes (tokens, IDs, URLs, raw payloads) are leaked into metrics."""
        response = client.get("/metrics")
        body = response.text

        # Ensure prohibited high-cardinality keys are not present as label keys
        assert 'delivery_id="' not in body
        assert 'event_id="' not in body
        assert 'api_key="' not in body
        assert 'url="' not in body
        assert 'payload="' not in body
        assert 'secret="' not in body

