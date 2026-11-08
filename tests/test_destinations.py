"""Destination endpoint tests."""
import uuid


def test_create_destination(client, auth_headers):
    """POST /v1/destinations creates a destination and returns 201."""
    response = client.post(
        "/v1/destinations",
        json={"name": "My Webhook", "url": "https://example.com/webhook"},
        headers=auth_headers,
    )
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "My Webhook"
    assert data["url"] == "https://example.com/webhook"
    assert data["is_active"] is True
    assert "id" in data
    # Signing secret must NOT appear in the response
    assert "signing_secret" not in data


def test_create_destination_invalid_url_scheme(client, auth_headers):
    """Destinations with non-http(s) schemes should be rejected."""
    response = client.post(
        "/v1/destinations",
        json={"name": "Bad", "url": "ftp://example.com/file"},
        headers=auth_headers,
    )
    assert response.status_code == 422


def test_create_destination_private_ip(client, auth_headers):
    """Destinations pointing to private IPs should be rejected."""
    response = client.post(
        "/v1/destinations",
        json={"name": "Private", "url": "http://192.168.1.1/webhook"},
        headers=auth_headers,
    )
    assert response.status_code == 422


def test_create_destination_loopback(client, auth_headers):
    """Destinations pointing to localhost should be rejected (unless allowlisted)."""
    # Since conftest sets ALLOW_LOCAL_TEST_DESTINATIONS to include localhost,
    # localhost is actually allowed in test mode. Test with 127.0.0.1 instead.
    response = client.post(
        "/v1/destinations",
        json={"name": "Loopback", "url": "http://127.0.0.1/webhook"},
        headers=auth_headers,
    )
    assert response.status_code == 422


def test_list_destinations(client, auth_headers, sample_destination):
    """GET /v1/destinations returns the tenant's destinations."""
    response = client.get("/v1/destinations", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 1
    assert len(data["items"]) >= 1
    # Verify no signing secrets in response
    for item in data["items"]:
        assert "signing_secret" not in item


def test_list_destinations_pagination(client, auth_headers, db_session, demo_tenant):
    """Pagination parameters should work correctly."""
    # Create multiple destinations
    from app.models.destination import Destination
    for i in range(5):
        dest = Destination(
            tenant_id=demo_tenant.id,
            name=f"Dest {i}",
            url=f"https://example.com/webhook/{i}",
            is_active=True,
        )
        db_session.add(dest)
    db_session.flush()

    response = client.get("/v1/destinations?offset=0&limit=2", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 2
    assert data["total"] >= 5


def test_list_destinations_cross_tenant_isolation(
    client, auth_headers, other_auth_headers, sample_destination
):
    """A tenant should not see another tenant's destinations."""
    response = client.get("/v1/destinations", headers=other_auth_headers)
    assert response.status_code == 200
    data = response.json()
    # The other tenant should have 0 destinations
    assert data["total"] == 0
