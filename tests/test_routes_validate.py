"""Tests for query validation routes."""

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from ggsql_rest._errors import register_error_handlers
from ggsql_rest._sessions import SessionManager
from ggsql_rest._connections import ConnectionRegistry
from ggsql_rest._routes._sessions import router as sessions_router, get_session_manager
from ggsql_rest._routes._query import router as query_router
from ggsql_rest._routes._dependencies import get_registry


def create_test_app() -> tuple[FastAPI, SessionManager, ConnectionRegistry]:
    app = FastAPI()
    session_mgr = SessionManager(timeout_mins=30)
    registry = ConnectionRegistry()

    app.dependency_overrides[get_session_manager] = lambda: session_mgr
    app.dependency_overrides[get_registry] = lambda: registry
    app.include_router(sessions_router)
    app.include_router(query_router)
    register_error_handlers(app)

    return app, session_mgr, registry


@pytest.mark.anyio
async def test_validate_single_valid_query():
    """A valid query returns valid: true."""
    app, _, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Create session
        create_resp = await client.post("/sessions")
        session_id = create_resp.json()["data"]["sessionId"]

        # Validate a valid query
        response = await client.post(
            f"/sessions/{session_id}/validate",
            json={"queries": [{"query": "SELECT 1 AS x"}]},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        results = body["data"]["results"]
        assert len(results) == 1
        assert results[0]["valid"] is True
        assert "error" not in results[0] or results[0]["error"] is None


@pytest.mark.anyio
async def test_validate_invalid_ggsql_syntax():
    """A query with ggsql parse errors returns valid: false with error message."""
    app, _, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post("/sessions")
        session_id = create_resp.json()["data"]["sessionId"]

        # Query with invalid ggsql syntax (typo in SELECT)
        response = await client.post(
            f"/sessions/{session_id}/validate",
            json={"queries": [{"query": "SELEC * FROM test VISUALISE x DRAW point"}]},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        results = body["data"]["results"]
        assert len(results) == 1
        assert results[0]["valid"] is False
        assert "error" in results[0]
        assert "Parse error" in results[0]["error"]


@pytest.mark.anyio
async def test_validate_nonexistent_column():
    """A query referencing a non-existent column returns valid: false."""
    app, _, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post("/sessions")
        session_id = create_resp.json()["data"]["sessionId"]

        # Create a table first
        await client.post(
            f"/sessions/{session_id}/sql",
            json={"query": "CREATE TABLE test_data AS SELECT 1 AS x, 2 AS y"},
        )

        # Query with non-existent column
        response = await client.post(
            f"/sessions/{session_id}/validate",
            json={"queries": [{"query": "SELECT nonexistent_col FROM test_data"}]},
        )

        assert response.status_code == 200
        body = response.json()
        results = body["data"]["results"]
        assert len(results) == 1
        assert results[0]["valid"] is False
        assert "error" in results[0]
        # DuckDB error should mention the column not being found
        assert "nonexistent_col" in results[0]["error"].lower()


@pytest.mark.anyio
async def test_validate_batch_mixed_valid_invalid():
    """Batch validation returns per-query results with correct valid/invalid status."""
    app, _, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post("/sessions")
        session_id = create_resp.json()["data"]["sessionId"]

        # Create a table
        await client.post(
            f"/sessions/{session_id}/sql",
            json={"query": "CREATE TABLE test_data AS SELECT 1 AS x, 2 AS y"},
        )

        # Batch with mix of valid and invalid queries
        response = await client.post(
            f"/sessions/{session_id}/validate",
            json={
                "queries": [
                    {"query": "SELECT * FROM test_data"},  # Valid
                    {"query": "SELEC * FROM test_data"},  # Invalid syntax
                    {"query": "SELECT x FROM test_data"},  # Valid
                    {"query": "SELECT bad_col FROM test_data"},  # Invalid column
                ]
            },
        )

        assert response.status_code == 200
        body = response.json()
        results = body["data"]["results"]
        assert len(results) == 4

        # First query: valid
        assert results[0]["valid"] is True

        # Second query: invalid syntax
        assert results[1]["valid"] is False
        assert "error" in results[1]

        # Third query: valid
        assert results[2]["valid"] is True

        # Fourth query: invalid column
        assert results[3]["valid"] is False
        assert "error" in results[3]


@pytest.mark.anyio
async def test_validate_query_with_visualise():
    """Queries with VISUALISE clause validate the SQL portion."""
    app, _, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post("/sessions")
        session_id = create_resp.json()["data"]["sessionId"]

        # Create a table
        await client.post(
            f"/sessions/{session_id}/sql",
            json={"query": "CREATE TABLE test_data AS SELECT 1 AS x, 2 AS y"},
        )

        # Valid query with VISUALISE
        response = await client.post(
            f"/sessions/{session_id}/validate",
            json={
                "queries": [
                    {"query": "SELECT x, y FROM test_data VISUALISE x, y DRAW point"}
                ]
            },
        )

        assert response.status_code == 200
        body = response.json()
        results = body["data"]["results"]
        assert len(results) == 1
        assert results[0]["valid"] is True

        # Invalid query with VISUALISE (bad column in SQL portion)
        response = await client.post(
            f"/sessions/{session_id}/validate",
            json={
                "queries": [
                    {
                        "query": "SELECT nonexistent FROM test_data VISUALISE x, y DRAW point"
                    }
                ]
            },
        )

        assert response.status_code == 200
        body = response.json()
        results = body["data"]["results"]
        assert len(results) == 1
        assert results[0]["valid"] is False
        assert "error" in results[0]


@pytest.mark.anyio
async def test_validate_session_not_found():
    """Validation on non-existent session returns 404."""
    app, _, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/sessions/nonexistent/validate",
            json={"queries": [{"query": "SELECT 1"}]},
        )

        assert response.status_code == 404
