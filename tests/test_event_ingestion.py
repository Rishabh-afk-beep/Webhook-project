"""Event ingestion tests."""
import uuid


def test_create_event_success(client, auth_headers, sample_destination):
    """POST /v1/events with valid input returns 202."""
    response = client.post(
        "/v1/events",
        json={
            "event_type": "order.created",
            "payload": {"order_id": "123", "amount": 2500},
            "destination_ids": [str(sample_destination.id)],
        },
        headers=auth_headers,
    )
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "accepted"
    assert data["delivery_count"] == 1
    assert len(data["deliveries"]) == 1
    assert data["deliveries"][0]["status"] == "pending"
    assert "event_id" in data


def test_create_event_multiple_destinations(client, auth_headers, db_session, demo_tenant):
    """Events sent to multiple destinations should create multiple deliveries."""
    from app.models.destination import Destination

    dests = []
    for i in range(3):
        d = Destination(
            tenant_id=demo_tenant.id,
            name=f"Multi Dest {i}",
            url=f"https://example.com/hook/{i}",
            is_active=True,
        )
        db_session.add(d)
        db_session.flush()
        dests.append(d)

    response = client.post(
        "/v1/events",
        json={
            "event_type": "order.shipped",
            "payload": {"tracking": "ABC"},
            "destination_ids": [str(d.id) for d in dests],
        },
        headers=auth_headers,
    )
    assert response.status_code == 202
    data = response.json()
    assert data["delivery_count"] == 3
    assert len(data["deliveries"]) == 3


def test_create_event_empty_destinations(client, auth_headers):
    """Events with no destinations should be rejected."""
    response = client.post(
        "/v1/events",
        json={
            "event_type": "test.event",
            "payload": {"key": "value"},
            "destination_ids": [],
        },
        headers=auth_headers,
    )
    assert response.status_code == 422


def test_create_event_nonexistent_destination(client, auth_headers):
    """Events referencing nonexistent destinations should be rejected."""
    response = client.post(
        "/v1/events",
        json={
            "event_type": "test.event",
            "payload": {"key": "value"},
            "destination_ids": [str(uuid.uuid4())],
        },
        headers=auth_headers,
    )
    assert response.status_code == 422


def test_create_event_other_tenants_destination(
    client, auth_headers, db_session, other_tenant
):
    """Events targeting another tenant's destination should be rejected."""
    from app.models.destination import Destination
    other_dest = Destination(
        tenant_id=other_tenant.id,
        name="Other's Dest",
        url="https://example.com/other",
        is_active=True,
    )
    db_session.add(other_dest)
    db_session.flush()

    response = client.post(
        "/v1/events",
        json={
            "event_type": "test.event",
            "payload": {"key": "value"},
            "destination_ids": [str(other_dest.id)],
        },
        headers=auth_headers,
    )
    assert response.status_code == 422


def test_create_event_inactive_destination(client, auth_headers, inactive_destination):
    """Events targeting an inactive destination should be rejected."""
    response = client.post(
        "/v1/events",
        json={
            "event_type": "test.event",
            "payload": {"key": "value"},
            "destination_ids": [str(inactive_destination.id)],
        },
        headers=auth_headers,
    )
    assert response.status_code == 422


def test_create_event_invalid_payload_type(client, auth_headers, sample_destination):
    """Payload must be a JSON object, not a string or array."""
    response = client.post(
        "/v1/events",
        json={
            "event_type": "test.event",
            "payload": "not a dict",
            "destination_ids": [str(sample_destination.id)],
        },
        headers=auth_headers,
    )
    assert response.status_code == 422


def test_get_event(client, auth_headers, sample_destination):
    """GET /v1/events/{id} returns the event with delivery summary."""
    # First create an event
    create_resp = client.post(
        "/v1/events",
        json={
            "event_type": "order.created",
            "payload": {"order_id": "456"},
            "destination_ids": [str(sample_destination.id)],
        },
        headers=auth_headers,
    )
    event_id = create_resp.json()["event_id"]

    # Then retrieve it
    response = client.get(f"/v1/events/{event_id}", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == event_id
    assert data["event_type"] == "order.created"
    assert data["payload"]["order_id"] == "456"


def test_get_event_not_found(client, auth_headers):
    """Getting a nonexistent event should return 404."""
    response = client.get(f"/v1/events/{uuid.uuid4()}", headers=auth_headers)
    assert response.status_code == 404


def test_get_event_cross_tenant(client, auth_headers, other_auth_headers, sample_destination):
    """A tenant should not be able to retrieve another tenant's events."""
    create_resp = client.post(
        "/v1/events",
        json={
            "event_type": "secret.event",
            "payload": {"secret": "data"},
            "destination_ids": [str(sample_destination.id)],
        },
        headers=auth_headers,
    )
    event_id = create_resp.json()["event_id"]

    # Other tenant trying to access
    response = client.get(f"/v1/events/{event_id}", headers=other_auth_headers)
    assert response.status_code == 404
