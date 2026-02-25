"""Tests for query routes."""

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

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
            json={"query": "SELECT 1 VISUALISE x DRAW point", "source": "nope"},
        )
        assert response.status_code == 400
        body = response.json()
        assert body["status"] == "error"
        assert body["error"]["type"] == "ConnectionNotFound"


@pytest.mark.anyio
async def test_sql_database_error_returns_502():
    """Database errors (e.g., bad SQL on remote) return 502 with DatabaseError type."""
    app, session_mgr, registry = create_test_app()

    # Register a connection with a real engine, then send bad SQL
    engine = create_engine("sqlite:///:memory:")
    registry.register("test_db", lambda _req: engine)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post("/sessions")
        session_id = create_resp.json()["data"]["sessionId"]

        response = await client.post(
            f"/sessions/{session_id}/sql",
            json={"query": "SELECT * FROM nonexistent_table", "source": "test_db"},
        )
        assert response.status_code == 502
        body = response.json()
        assert body["status"] == "error"
        assert body["error"]["type"] == "DatabaseError"


@pytest.mark.anyio
async def test_sql_accepts_timeout_seconds():
    """The /sql endpoint should accept an optional timeoutSeconds parameter."""
    app, session_mgr, registry = create_test_app()

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER)"))
        conn.execute(text("INSERT INTO t VALUES (1)"))

    registry.register("test_db", lambda _req: engine)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post("/sessions")
        session_id = create_resp.json()["data"]["sessionId"]

        response = await client.post(
            f"/sessions/{session_id}/sql",
            json={
                "query": "SELECT * FROM t",
                "source": "test_db",
                "timeoutSeconds": 30,
            },
        )
        assert response.status_code == 200


@pytest.mark.anyio
async def test_query_response_includes_truncated():
    """The /query response metadata should include a truncated field."""
    app, session_mgr, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post("/sessions")
        session_id = create_resp.json()["data"]["sessionId"]

        response = await client.post(
            f"/sessions/{session_id}/query",
            json={
                "query": "SELECT * FROM (VALUES (1, 2), (3, 4)) AS t(x, y) VISUALISE x, y DRAW point"
            },
        )
        assert response.status_code == 200
        metadata = response.json()["data"]["metadata"]
        assert "truncated" in metadata
        assert metadata["truncated"] is False


@pytest.mark.anyio
async def test_query_with_snowflake_source():
    """When source resolves to Snowflake, query should work via engine fallback."""
    from unittest.mock import MagicMock
    from ggsql_rest._routes._dependencies import get_snowflake_discovery

    app, session_mgr, registry = create_test_app()

    mock_snowflake = MagicMock()
    mock_snowflake.has_connection.return_value = True
    mock_snowflake.has_adbc_support.return_value = False

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (x INTEGER, y INTEGER)"))
        conn.execute(text("INSERT INTO t VALUES (1, 2), (3, 4)"))
    mock_snowflake.get_engine.return_value = engine

    app.dependency_overrides[get_snowflake_discovery] = lambda: mock_snowflake

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post("/sessions")
        session_id = create_resp.json()["data"]["sessionId"]

        response = await client.post(
            f"/sessions/{session_id}/query",
            json={
                "query": "SELECT * FROM t VISUALISE x, y DRAW point",
                "source": "MY_DB.PUBLIC",
            },
        )
        assert response.status_code == 200
        data = response.json()["data"]
        assert "spec" in data
