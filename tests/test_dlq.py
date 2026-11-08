"""Tests for Dead-Letter Queue (DLQ) and manual replay."""
import uuid
from datetime import datetime, timezone
import pytest
from fastapi.testclient import TestClient

from app.models.delivery import EventDelivery
from app.models.delivery_attempt import DeliveryAttempt
from app.models.destination import Destination
from app.models.event import Event


def _create_dead_letter(db_session, tenant, destination, attempt_count=5):
    """Helper to populate a dead-lettered delivery with attempts."""
    event = Event(
        tenant_id=tenant.id,
        event_type="order.failed",
        payload={"order_id": "ord_dlq_1", "amount": 100},
    )
    db_session.add(event)
    db_session.flush()

    delivery = EventDelivery(
        tenant_id=tenant.id,
        event_id=event.id,
        destination_id=destination.id,
        status="dead_lettered",
        attempt_count=attempt_count,
        dead_lettered_at=datetime.now(timezone.utc),
        last_http_status=500,
        last_error="HTTP 500: Server Error",
    )
    db_session.add(delivery)
    db_session.flush()

    # Add historical attempts
    for i in range(1, attempt_count + 1):
        att = DeliveryAttempt(
            delivery_id=delivery.id,
            attempt_number=i,
            outcome="failed",
            http_status=500,
            response_time_ms=50,
            error_message="HTTP 500",
            error_category="server_error",
            started_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
        )
        db_session.add(att)

    db_session.commit()
    return event, delivery


class TestDeadLetterQueue:
    """Test DLQ listing, retrieval, and replay endpoints."""

    def test_dlq_list_empty(self, client: TestClient, auth_headers: dict):
        """Empty DLQ returns 200 with 0 items."""
        response = client.get("/v1/dead-letters", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0
        assert data["items"] == []

    def test_dlq_list_pagination(
        self, client: TestClient, auth_headers: dict, db_session, demo_tenant
    ):
        """Requirement: DLQ list endpoint supports pagination."""
        dest = Destination(tenant_id=demo_tenant.id, name="DLQ Dest", url="http://example.com/hook")
        db_session.add(dest)
        db_session.flush()

        for _ in range(5):
            _create_dead_letter(db_session, demo_tenant, dest)

        # Page 1: limit 2
        r1 = client.get("/v1/dead-letters?limit=2&offset=0", headers=auth_headers)
        assert r1.status_code == 200
        d1 = r1.json()
        assert d1["total"] == 5
        assert len(d1["items"]) == 2
        assert d1["limit"] == 2
        assert d1["offset"] == 0

        # Page 2: limit 2, offset 2
        r2 = client.get("/v1/dead-letters?limit=2&offset=2", headers=auth_headers)
        assert r2.status_code == 200
        d2 = r2.json()
        assert len(d2["items"]) == 2

        # Page 3: offset 4
        r3 = client.get("/v1/dead-letters?limit=2&offset=4", headers=auth_headers)
        assert r3.status_code == 200
        d3 = r3.json()
        assert len(d3["items"]) == 1

    def test_tenant_cannot_inspect_other_tenants_dead_letters(
        self,
        client: TestClient,
        auth_headers: dict,
        other_api_key: tuple,
        db_session,
        other_tenant,
    ):
        """Requirement: Cross-tenant isolation on DLQ listing."""
        other_raw_key, _ = other_api_key
        other_dest = Destination(
            tenant_id=other_tenant.id, name="Other Dest", url="http://other.com/hook"
        )
        db_session.add(other_dest)
        db_session.flush()

        _create_dead_letter(db_session, other_tenant, other_dest)

        # First tenant queries DLQ -> should see 0
        r = client.get("/v1/dead-letters", headers=auth_headers)
        assert r.status_code == 200
        assert r.json()["total"] == 0

        # Other tenant queries DLQ -> should see 1
        r_other = client.get(
            "/v1/dead-letters",
            headers={"Authorization": f"Bearer {other_raw_key}"},
        )
        assert r_other.status_code == 200
        assert r_other.json()["total"] == 1

    def test_get_dead_letter_by_id(
        self, client: TestClient, auth_headers: dict, db_session, demo_tenant
    ):
        """Retrieve a specific dead-letter with full attempt history."""
        dest = Destination(tenant_id=demo_tenant.id, name="D", url="http://example.com/h")
        db_session.add(dest)
        db_session.flush()

        _, delivery = _create_dead_letter(db_session, demo_tenant, dest, attempt_count=3)

        r = client.get(f"/v1/dead-letters/{delivery.id}", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert data["id"] == str(delivery.id)
        assert data["status"] == "dead_lettered"
        assert len(data["attempts"]) == 3
        assert data["attempts"][0]["error_category"] == "server_error"

    def test_unauthorized_replay_rejected(self, client: TestClient):
        """Requirement: Unauthorized replay requests return 401."""
        random_id = uuid.uuid4()
        r = client.post(f"/v1/dead-letters/{random_id}/replay")
        assert r.status_code in (401, 403)

    def test_eligible_delivery_replayed_successfully(
        self, client: TestClient, auth_headers: dict, db_session, demo_tenant
    ):
        """Requirement: Eligible dead-lettered delivery can be replayed."""
        dest = Destination(tenant_id=demo_tenant.id, name="D", url="http://example.com/h")
        db_session.add(dest)
        db_session.flush()

        _, orig_delivery = _create_dead_letter(db_session, demo_tenant, dest, attempt_count=5)

        r = client.post(f"/v1/dead-letters/{orig_delivery.id}/replay", headers=auth_headers)
        assert r.status_code == 202
        data = r.json()
        assert data["original_delivery_id"] == str(orig_delivery.id)
        assert "new_delivery_id" in data

        new_delivery_id = uuid.UUID(data["new_delivery_id"])
        db_session.expire_all()
        new_del = db_session.get(EventDelivery, new_delivery_id)
        orig_del = db_session.get(EventDelivery, orig_delivery.id)

        # New delivery is pending and linked to original
        assert new_del.status == "pending"
        assert new_del.replay_of_delivery_id == orig_delivery.id
        assert new_del.event_id == orig_delivery.event_id
        assert new_del.destination_id == orig_delivery.destination_id

        # Original delivery recorded replay timestamp
        assert orig_del.replayed_at is not None
        assert orig_del.status == "dead_lettered"
        # Original attempt history is preserved (5 attempts)
        assert len(orig_del.attempts) == 5

    def test_concurrent_replay_rejected(
        self, client: TestClient, auth_headers: dict, db_session, demo_tenant
    ):
        """Requirement: Replaying an already-replayed delivery returns 409 Conflict."""
        dest = Destination(tenant_id=demo_tenant.id, name="D", url="http://example.com/h")
        db_session.add(dest)
        db_session.flush()

        _, orig_delivery = _create_dead_letter(db_session, demo_tenant, dest)

        # First replay succeeds
        r1 = client.post(f"/v1/dead-letters/{orig_delivery.id}/replay", headers=auth_headers)
        assert r1.status_code == 202

        # Second replay must be rejected
        r2 = client.post(f"/v1/dead-letters/{orig_delivery.id}/replay", headers=auth_headers)
        assert r2.status_code == 409
        assert "already been replayed" in r2.json()["detail"]

    def test_ineligible_delivery_cannot_be_replayed(
        self, client: TestClient, auth_headers: dict, db_session, demo_tenant
    ):
        """Requirement: Non-dead-lettered delivery (e.g. pending/succeeded) returns 422."""
        event = Event(tenant_id=demo_tenant.id, event_type="test", payload={"k": "v"})
        dest = Destination(tenant_id=demo_tenant.id, name="D", url="http://example.com/h")
        db_session.add_all([event, dest])
        db_session.flush()

        active_delivery = EventDelivery(
            tenant_id=demo_tenant.id,
            event_id=event.id,
            destination_id=dest.id,
            status="succeeded",
        )
        db_session.add(active_delivery)
        db_session.commit()

        r = client.post(f"/v1/dead-letters/{active_delivery.id}/replay", headers=auth_headers)
        assert r.status_code == 422
        assert "not dead-lettered" in r.json()["detail"]

    def test_tenant_cannot_replay_other_tenants_delivery(
        self,
        client: TestClient,
        auth_headers: dict,
        other_tenant,
        db_session,
    ):
        """Requirement: A tenant cannot replay another tenant's delivery."""
        dest = Destination(tenant_id=other_tenant.id, name="D", url="http://other.com/h")
        db_session.add(dest)
        db_session.flush()

        _, other_delivery = _create_dead_letter(db_session, other_tenant, dest)

        r = client.post(f"/v1/dead-letters/{other_delivery.id}/replay", headers=auth_headers)
        assert r.status_code == 404

