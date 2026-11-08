"""Integration tests.

These tests verify the end-to-end flow from event ingestion through
outbox processing and would ideally run against real PostgreSQL and Redis.

When run with SQLite (the default test configuration), they verify
the API and service layer behavior. Full integration testing against
Docker services requires TEST_DATABASE_URL pointing to a real PostgreSQL.

To run integration tests against Docker services:
    TEST_DATABASE_URL=postgresql+psycopg://webhook_user:webhook_pass@localhost:5432/webhook_test_db \
    pytest tests/test_integration.py -v
"""
import uuid

from sqlalchemy import select

from app.models.event import Event
from app.models.delivery import EventDelivery
from app.models.outbox import OutboxEntry


class TestTransactionalIngestion:
    """Verify the transactional guarantees of event ingestion."""

    def test_event_creates_delivery_and_outbox_atomically(
        self, client, auth_headers, sample_destination, db_session
    ):
        """A single event submission must create event + delivery + outbox in one transaction."""
        response = client.post(
            "/v1/events",
            json={
                "event_type": "integration.test",
                "payload": {"test": True},
                "destination_ids": [str(sample_destination.id)],
            },
            headers=auth_headers,
        )
        assert response.status_code == 202
        data = response.json()

        event_id = uuid.UUID(data["event_id"])
        delivery_id = uuid.UUID(data["deliveries"][0]["delivery_id"])

        # Verify event exists
        event = db_session.get(Event, event_id)
        assert event is not None
        assert event.event_type == "integration.test"

        # Verify delivery exists
        delivery = db_session.get(EventDelivery, delivery_id)
        assert delivery is not None
        assert delivery.status == "pending"
        assert delivery.event_id == event_id
        assert delivery.destination_id == sample_destination.id

        # Verify outbox entry exists
        outbox = db_session.execute(
            select(OutboxEntry).where(OutboxEntry.delivery_id == delivery_id)
        ).scalar_one_or_none()
        assert outbox is not None
        assert outbox.status == "pending"

    def test_full_event_lifecycle_via_api(
        self, client, auth_headers, sample_destination, db_session
    ):
        """Test the complete API flow: create event -> check event -> check delivery."""
        # Step 1: Create event
        create_resp = client.post(
            "/v1/events",
            json={
                "event_type": "order.created",
                "payload": {"order_id": "int-test-001", "amount": 5000, "currency": "INR"},
                "destination_ids": [str(sample_destination.id)],
            },
            headers=auth_headers,
        )
        assert create_resp.status_code == 202
        create_data = create_resp.json()
        event_id = create_data["event_id"]
        delivery_id = create_data["deliveries"][0]["delivery_id"]

        # Step 2: Retrieve event
        event_resp = client.get(f"/v1/events/{event_id}", headers=auth_headers)
        assert event_resp.status_code == 200
        event_data = event_resp.json()
        assert event_data["event_type"] == "order.created"
        assert event_data["payload"]["order_id"] == "int-test-001"
        assert len(event_data["deliveries"]) == 1

        # Step 3: Check delivery status
        delivery_resp = client.get(f"/v1/deliveries/{delivery_id}", headers=auth_headers)
        assert delivery_resp.status_code == 200
        delivery_data = delivery_resp.json()
        assert delivery_data["status"] == "pending"
        assert delivery_data["event_id"] == event_id

        # Step 4: Check attempts (should be empty before worker runs)
        attempts_resp = client.get(
            f"/v1/deliveries/{delivery_id}/attempts", headers=auth_headers
        )
        assert attempts_resp.status_code == 200
        assert attempts_resp.json()["total"] == 0

    def test_multi_destination_creates_separate_records(
        self, client, auth_headers, db_session, demo_tenant
    ):
        """3 destinations should produce 3 deliveries and 3 outbox entries."""
        from app.models.destination import Destination

        dests = []
        for i in range(3):
            d = Destination(
                tenant_id=demo_tenant.id,
                name=f"IntDest {i}",
                url=f"https://example.com/int/{i}",
                is_active=True,
            )
            db_session.add(d)
            db_session.flush()
            dests.append(d)

        response = client.post(
            "/v1/events",
            json={
                "event_type": "multi.test",
                "payload": {"count": 3},
                "destination_ids": [str(d.id) for d in dests],
            },
            headers=auth_headers,
        )
        assert response.status_code == 202
        data = response.json()
        assert data["delivery_count"] == 3

        # Verify 3 deliveries and 3 outbox entries in DB
        event_id = uuid.UUID(data["event_id"])
        deliveries = list(db_session.execute(
            select(EventDelivery).where(EventDelivery.event_id == event_id)
        ).scalars().all())
        assert len(deliveries) == 3

        outbox_entries = list(db_session.execute(
            select(OutboxEntry).where(
                OutboxEntry.delivery_id.in_([d.id for d in deliveries])
            )
        ).scalars().all())
        assert len(outbox_entries) == 3


class TestTenantIsolation:
    """Verify that tenants cannot access each other's resources."""

    def test_cannot_list_other_tenants_destinations(
        self, client, auth_headers, other_auth_headers, sample_destination
    ):
        response = client.get("/v1/destinations", headers=other_auth_headers)
        assert response.status_code == 200
        assert response.json()["total"] == 0

    def test_cannot_read_other_tenants_event(
        self, client, auth_headers, other_auth_headers, sample_destination
    ):
        create_resp = client.post(
            "/v1/events",
            json={
                "event_type": "secret",
                "payload": {"data": "sensitive"},
                "destination_ids": [str(sample_destination.id)],
            },
            headers=auth_headers,
        )
        event_id = create_resp.json()["event_id"]

        get_resp = client.get(f"/v1/events/{event_id}", headers=other_auth_headers)
        assert get_resp.status_code == 404

    def test_cannot_read_other_tenants_delivery(
        self, client, auth_headers, other_auth_headers, sample_destination
    ):
        create_resp = client.post(
            "/v1/events",
            json={
                "event_type": "secret",
                "payload": {"data": "sensitive"},
                "destination_ids": [str(sample_destination.id)],
            },
            headers=auth_headers,
        )
        delivery_id = create_resp.json()["deliveries"][0]["delivery_id"]

        get_resp = client.get(f"/v1/deliveries/{delivery_id}", headers=other_auth_headers)
        assert get_resp.status_code == 404

    def test_cannot_deliver_to_other_tenants_destination(
        self, client, other_auth_headers, sample_destination
    ):
        response = client.post(
            "/v1/events",
            json={
                "event_type": "attack",
                "payload": {"data": "malicious"},
                "destination_ids": [str(sample_destination.id)],
            },
            headers=other_auth_headers,
        )
        assert response.status_code == 422
