"""Tests for query routes."""

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
async def test_execute_query_local():
    app, session_mgr, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Create session via API
        create_resp = await client.post("/sessions")
        assert create_resp.status_code == 200
        body = create_resp.json()
        assert body["status"] == "success"
        session_id = body["data"]["sessionId"]

        # Query with inline data (no need to pre-create table)
        response = await client.post(
            f"/sessions/{session_id}/query",
            json={
                "query": "SELECT * FROM (VALUES (1, 2), (3, 4)) AS test(x, y) VISUALISE x, y DRAW point"
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        data = body["data"]
        assert "spec" in data
        assert "metadata" in data


@pytest.mark.anyio
async def test_execute_query_session_not_found():
    app, _, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/sessions/nonexistent/query",
            json={"query": "SELECT * FROM test VISUALISE x, y DRAW point"},
        )

        assert response.status_code == 404


@pytest.mark.anyio
async def test_execute_sql_local():
    app, session_mgr, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Create session via API
        create_resp = await client.post("/sessions")
        assert create_resp.status_code == 200
        body = create_resp.json()
        assert body["status"] == "success"
        session_id = body["data"]["sessionId"]

        # Query with inline data
        response = await client.post(
            f"/sessions/{session_id}/sql",
            json={"query": "SELECT * FROM (VALUES (1, 2), (3, 4)) AS test(x, y)"},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        data = body["data"]
        assert "rows" in data
        assert "columns" in data
        assert len(data["rows"]) == 2


@pytest.mark.anyio
async def test_query_without_visualise_returns_400():
    app, session_mgr, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post("/sessions")
        body = create_resp.json()
        assert body["status"] == "success"
        session_id = body["data"]["sessionId"]

        response = await client.post(
            f"/sessions/{session_id}/query",
            json={"query": "SELECT 1 AS x"},
        )
        assert response.status_code == 400
        body = response.json()
        assert body["status"] == "error"


@pytest.mark.anyio
async def test_query_unknown_connection_returns_400():
    app, session_mgr, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post("/sessions")
        body = create_resp.json()
        assert body["status"] == "success"
        session_id = body["data"]["sessionId"]

        response = await client.post(
            f"/sessions/{session_id}/query",
            json={"query": "SELECT 1 VISUALISE x DRAW point", "connection": "nope"},
        )
        assert response.status_code == 400
        body = response.json()
        assert body["status"] == "error"
        assert body["error"]["type"] == "ConnectionNotFound"
