"""Tests for app factory."""

import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient, ASGITransport

from ggsql_rest import create_app, ConnectionRegistry


def test_create_app():
    registry = ConnectionRegistry()
    app = create_app(registry)

    client = TestClient(app)

    # Health check should work at /api/v1 prefix
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_create_app_with_cors():
    registry = ConnectionRegistry()
    app = create_app(registry, cors_origins=["http://localhost:3000"])

    # CORS headers should be present
    client = TestClient(app)
    response = client.options(
        "/api/v1/health",
        headers={"Origin": "http://localhost:3000"},
    )
    assert "access-control-allow-origin" in response.headers


@pytest.mark.anyio
async def test_full_workflow():
    """Test full workflow: create session, query with inline data, delete."""
    registry = ConnectionRegistry()
    app = create_app(registry)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        # Create session
        response = await client.post("/api/v1/sessions")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        session_id = body["data"]["sessionId"]

        # Query with inline data (avoids DuckDB thread safety issues in async tests)
        response = await client.post(
            f"/api/v1/sessions/{session_id}/query",
            json={
                "query": "SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) AS test(x, y) VISUALISE x, y DRAW point"
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        data = body["data"]
        assert "spec" in data
        assert "metadata" in data

        # Delete session
        response = await client.delete(f"/api/v1/sessions/{session_id}")
        assert response.status_code == 200
        assert response.json() == {"status": "success", "data": None}


def test_version_matches_pyproject():
    """Version in __init__ should come from pyproject.toml via metadata."""
    from importlib.metadata import version
    import ggsql_rest

    assert ggsql_rest.__version__ == version("ggsql-rest")


def test_shutdown_disposes_engines():
    """Verify engine disposal runs on app shutdown."""
    from unittest.mock import patch

    registry = ConnectionRegistry()
    app = create_app(registry)

    # Patch dispose_all and trigger shutdown via lifespan
    with patch.object(registry, "dispose_all") as mock_dispose:
        with TestClient(app):
            # Verify state is set during lifespan startup
            assert app.state.registry is registry
            assert app.state.session_manager is not None
        # After exiting TestClient, shutdown should have called dispose_all
        mock_dispose.assert_called_once()
