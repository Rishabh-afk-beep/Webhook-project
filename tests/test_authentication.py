"""Authentication tests."""


def test_missing_api_key(client):
    """Requests without an API key should return 401 or 403."""
    response = client.get("/v1/destinations")
    assert response.status_code in (401, 403)


def test_invalid_api_key(client):
    """Requests with an invalid API key should return 401."""
    response = client.get(
        "/v1/destinations",
        headers={"Authorization": "Bearer invalid-key-that-does-not-exist"},
    )
    assert response.status_code == 401


def test_valid_api_key(client, auth_headers):
    """Requests with a valid API key should succeed."""
    response = client.get("/v1/destinations", headers=auth_headers)
    assert response.status_code == 200


def test_revoked_api_key(client, db_session, demo_api_key):
    """Revoked API keys should be rejected."""
    from datetime import datetime, timezone
    raw_key, api_key = demo_api_key
    api_key.revoked_at = datetime.now(timezone.utc)
    db_session.flush()

    response = client.get(
        "/v1/destinations",
        headers={"Authorization": f"Bearer {raw_key}"},
    )
    assert response.status_code == 401
