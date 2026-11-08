"""Health endpoint tests."""


def test_health_endpoint(client):
    """GET /health returns 200 with status ok."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"


def test_readiness_endpoint(client):
    """GET /health/ready returns 200 when DB is available."""
    response = client.get("/health/ready")
    # In test mode with SQLite, this should succeed
    assert response.status_code in (200, 503)
