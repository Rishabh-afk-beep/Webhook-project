"""Delivery status and attempt retrieval tests."""
import uuid


def test_get_delivery(client, auth_headers, sample_destination):
    """GET /v1/deliveries/{id} returns delivery status."""
    # Create an event first
    create_resp = client.post(
        "/v1/events",
        json={
            "event_type": "order.created",
            "payload": {"order_id": "789"},
            "destination_ids": [str(sample_destination.id)],
        },
        headers=auth_headers,
    )
    delivery_id = create_resp.json()["deliveries"][0]["delivery_id"]

    response = client.get(f"/v1/deliveries/{delivery_id}", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == delivery_id
    assert data["status"] == "pending"
    assert data["attempt_count"] == 0


def test_get_delivery_not_found(client, auth_headers):
    """Getting a nonexistent delivery should return 404."""
    response = client.get(f"/v1/deliveries/{uuid.uuid4()}", headers=auth_headers)
    assert response.status_code == 404


def test_get_delivery_cross_tenant(
    client, auth_headers, other_auth_headers, sample_destination
):
    """A tenant should not access another tenant's deliveries."""
    create_resp = client.post(
        "/v1/events",
        json={
            "event_type": "test.event",
            "payload": {"data": "x"},
            "destination_ids": [str(sample_destination.id)],
        },
        headers=auth_headers,
    )
    delivery_id = create_resp.json()["deliveries"][0]["delivery_id"]

    response = client.get(f"/v1/deliveries/{delivery_id}", headers=other_auth_headers)
    assert response.status_code == 404


def test_get_delivery_attempts_empty(client, auth_headers, sample_destination):
    """A new delivery should have no attempts yet."""
    create_resp = client.post(
        "/v1/events",
        json={
            "event_type": "test.event",
            "payload": {"data": "x"},
            "destination_ids": [str(sample_destination.id)],
        },
        headers=auth_headers,
    )
    delivery_id = create_resp.json()["deliveries"][0]["delivery_id"]

    response = client.get(
        f"/v1/deliveries/{delivery_id}/attempts", headers=auth_headers
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 0
    assert data["items"] == []
