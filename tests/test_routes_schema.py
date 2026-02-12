"""Tests for schema route."""

import io
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from ggsql_rest import ConnectionRegistry
from ggsql_rest._errors import register_error_handlers
from ggsql_rest._sessions import SessionManager
from ggsql_rest._routes._sessions import router as sessions_router
from ggsql_rest._routes._sessions import get_session_manager
from ggsql_rest._routes._query import router as query_router
from ggsql_rest._routes._schema import router as schema_router
from ggsql_rest._routes._dependencies import get_registry


def create_test_app(registry: ConnectionRegistry) -> tuple[FastAPI, SessionManager]:
    """Create test app with schema, sessions, and query routers."""
    app = FastAPI()
    session_mgr = SessionManager(timeout_mins=30)

    app.dependency_overrides[get_session_manager] = lambda: session_mgr
    app.dependency_overrides[get_registry] = lambda: registry

    app.include_router(sessions_router)
    app.include_router(query_router)
    app.include_router(schema_router)
    register_error_handlers(app)

    return app, session_mgr


@pytest.mark.anyio
async def test_schema_local_table():
    """Schema returns uploaded table columns."""
    app, session_mgr = create_test_app(ConnectionRegistry())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        csv_content = b"x,y,label\n1,10,a\n2,20,b\n3,30,a"
        files = {"file": ("data.csv", io.BytesIO(csv_content), "text/csv")}
        await client.post(f"/sessions/{session.id}/upload", files=files)

        response = await client.get(f"/sessions/{session.id}/schema")

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        tables = body["data"]["tables"]
        assert len(tables) == 1
        assert tables[0]["tableName"] == "_upload_data"
        assert tables[0]["connection"] is None
        assert len(tables[0]["columns"]) == 3


@pytest.mark.anyio
async def test_schema_with_stats():
    """Schema with include_stats returns column statistics."""
    app, session_mgr = create_test_app(ConnectionRegistry())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        csv_content = b"score,category\n10,A\n20,B\n30,A"
        files = {"file": ("data.csv", io.BytesIO(csv_content), "text/csv")}
        await client.post(f"/sessions/{session.id}/upload", files=files)

        response = await client.get(
            f"/sessions/{session.id}/schema?include_stats=true"
        )

        assert response.status_code == 200
        tables = response.json()["data"]["tables"]
        columns = {c["columnName"]: c for c in tables[0]["columns"]}

        # Numeric column should have min/max
        assert columns["score"]["minValue"] is not None
        assert columns["score"]["maxValue"] is not None


@pytest.mark.anyio
async def test_schema_with_remote_connection():
    """Schema includes tables from remote connections."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"))

    registry = ConnectionRegistry()
    registry.register("test_db", lambda req: engine)

    app, session_mgr = create_test_app(registry)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        response = await client.get(f"/sessions/{session.id}/schema")

        assert response.status_code == 200
        tables = response.json()["data"]["tables"]

        remote_tables = [t for t in tables if t["connection"] == "test_db"]
        assert len(remote_tables) == 1
        assert remote_tables[0]["tableName"] == "users"


@pytest.mark.anyio
async def test_schema_empty_session():
    """Schema with no tables returns empty list."""
    app, session_mgr = create_test_app(ConnectionRegistry())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        response = await client.get(f"/sessions/{session.id}/schema")

        assert response.status_code == 200
        assert response.json()["data"]["tables"] == []


@pytest.mark.anyio
async def test_schema_session_not_found():
    """Schema for nonexistent session returns 404."""
    app, _ = create_test_app(ConnectionRegistry())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/sessions/nonexistent/schema")
        assert response.status_code == 404
