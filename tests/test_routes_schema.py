"""Tests for schema route."""

import io
import json
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
    registry.register("test_db", lambda _req: engine)

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


@pytest.mark.anyio
async def test_schema_tables_local():
    """Schema tables endpoint returns just table names without columns."""
    app, session_mgr = create_test_app(ConnectionRegistry())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        # Upload a CSV to create a local table
        csv_content = b"x,y,label\n1,10,a\n2,20,b\n3,30,a"
        files = {"file": ("data.csv", io.BytesIO(csv_content), "text/csv")}
        await client.post(f"/sessions/{session.id}/upload", files=files)

        # Request table names
        response = await client.get(f"/sessions/{session.id}/schema/tables")

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        tables = body["data"]["tables"]
        assert len(tables) == 1
        assert tables[0]["tableName"] == "_upload_data"
        assert tables[0]["connection"] is None
        # Verify no columns are included
        assert "columns" not in tables[0]


@pytest.mark.anyio
async def test_schema_tables_with_remote():
    """Schema tables endpoint includes remote connection tables."""
    # Create a SQLite in-memory engine with a table
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"))

    registry = ConnectionRegistry()
    registry.register("test_db", lambda _req: engine)

    app, session_mgr = create_test_app(registry)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        # Request table names
        response = await client.get(f"/sessions/{session.id}/schema/tables")

        assert response.status_code == 200
        tables = response.json()["data"]["tables"]

        # Find the remote table
        remote_tables = [t for t in tables if t["connection"] == "test_db"]
        assert len(remote_tables) == 1
        assert remote_tables[0]["tableName"] == "users"
        # Verify no columns are included
        assert "columns" not in remote_tables[0]


@pytest.mark.anyio
async def test_schema_table_local():
    """Per-table schema endpoint returns local table columns."""
    app, session_mgr = create_test_app(ConnectionRegistry())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        csv_content = b"x,y,label\n1,10,a\n2,20,b\n3,30,a"
        files = {"file": ("data.csv", io.BytesIO(csv_content), "text/csv")}
        await client.post(f"/sessions/{session.id}/upload", files=files)

        response = await client.get(f"/sessions/{session.id}/schema/table/_upload_data")

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        table = body["data"]
        assert table["tableName"] == "_upload_data"
        assert table["connection"] is None
        assert len(table["columns"]) == 3
        column_names = {c["columnName"] for c in table["columns"]}
        assert column_names == {"x", "y", "label"}


@pytest.mark.anyio
async def test_schema_table_with_stats():
    """Per-table schema endpoint with include_stats returns column statistics."""
    app, session_mgr = create_test_app(ConnectionRegistry())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        csv_content = b"score,category\n10,A\n20,B\n30,A"
        files = {"file": ("data.csv", io.BytesIO(csv_content), "text/csv")}
        await client.post(f"/sessions/{session.id}/upload", files=files)

        response = await client.get(
            f"/sessions/{session.id}/schema/table/_upload_data?include_stats=true"
        )

        assert response.status_code == 200
        table = response.json()["data"]
        columns = {c["columnName"]: c for c in table["columns"]}

        # Numeric column should have min/max
        assert columns["score"]["minValue"] == "10"
        assert columns["score"]["maxValue"] == "30"

        # Categorical column should have values
        assert "categoricalValues" in columns["category"]
        assert set(columns["category"]["categoricalValues"]) == {"A", "B"}


@pytest.mark.anyio
async def test_schema_table_remote():
    """Per-table schema endpoint returns remote table columns."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"))

    registry = ConnectionRegistry()
    registry.register("test_db", lambda _req: engine)

    app, session_mgr = create_test_app(registry)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        response = await client.get(
            f"/sessions/{session.id}/schema/table/users?connection=test_db"
        )

        assert response.status_code == 200
        table = response.json()["data"]
        assert table["tableName"] == "users"
        assert table["connection"] == "test_db"
        column_names = {c["columnName"] for c in table["columns"]}
        assert column_names == {"id", "name"}


@pytest.mark.anyio
async def test_schema_table_not_found():
    """Per-table schema endpoint returns 404 for nonexistent table."""
    app, session_mgr = create_test_app(ConnectionRegistry())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        response = await client.get(f"/sessions/{session.id}/schema/table/nonexistent")
        assert response.status_code == 404


@pytest.mark.anyio
async def test_schema_tables_stream_local():
    """Schema tables stream endpoint returns NDJSON with local table."""
    app, session_mgr = create_test_app(ConnectionRegistry())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        # Upload a CSV to create a local table
        csv_content = b"x,y,label\n1,10,a\n2,20,b\n3,30,a"
        files = {"file": ("data.csv", io.BytesIO(csv_content), "text/csv")}
        await client.post(f"/sessions/{session.id}/upload", files=files)

        # Request streaming table names
        response = await client.get(f"/sessions/{session.id}/schema/tables?stream=true")

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/x-ndjson"

        # Parse NDJSON lines
        lines = response.text.strip().split("\n")
        assert len(lines) == 1

        first_line = json.loads(lines[0])
        assert "tables" in first_line
        assert len(first_line["tables"]) == 1
        assert first_line["tables"][0]["tableName"] == "_upload_data"
        assert first_line["tables"][0]["connection"] is None


@pytest.mark.anyio
async def test_schema_tables_stream_with_remote():
    """Schema tables stream endpoint includes remote tables in first line."""
    # Create a SQLite in-memory engine with a table
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"))

    registry = ConnectionRegistry()
    registry.register("test_db", lambda _req: engine)

    app, session_mgr = create_test_app(registry)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        # Request streaming table names
        response = await client.get(f"/sessions/{session.id}/schema/tables?stream=true")

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/x-ndjson"

        # Parse NDJSON lines
        lines = response.text.strip().split("\n")
        assert len(lines) == 1

        first_line = json.loads(lines[0])
        assert "tables" in first_line
        remote_tables = [t for t in first_line["tables"] if t["connection"] == "test_db"]
        assert len(remote_tables) == 1
        assert remote_tables[0]["tableName"] == "users"


@pytest.mark.anyio
async def test_schema_tables_stream_empty():
    """Schema tables stream endpoint returns empty response when no tables."""
    app, session_mgr = create_test_app(ConnectionRegistry())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        # Request streaming table names (no tables uploaded)
        response = await client.get(f"/sessions/{session.id}/schema/tables?stream=true")

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/x-ndjson"

        # Empty session should have empty response body
        assert response.text == ""


@pytest.mark.anyio
async def test_schema_tables_includes_provider():
    """Schema tables endpoint includes provider field for remote connections."""
    # Create a SQLite in-memory engine with a table
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"))

    registry = ConnectionRegistry()
    # Register with explicit provider
    registry.register("test_db", lambda _req: engine, provider="sqlite")

    app, session_mgr = create_test_app(registry)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        # Upload a local table
        csv_content = b"x,y,label\n1,10,a\n2,20,b\n3,30,a"
        files = {"file": ("data.csv", io.BytesIO(csv_content), "text/csv")}
        await client.post(f"/sessions/{session.id}/upload", files=files)

        # Request table names
        response = await client.get(f"/sessions/{session.id}/schema/tables")

        assert response.status_code == 200
        tables = response.json()["data"]["tables"]

        # Local table should have provider=None
        local_tables = [t for t in tables if t["connection"] is None]
        assert len(local_tables) == 1
        assert local_tables[0]["provider"] is None

        # Remote table should have provider="sqlite"
        remote_tables = [t for t in tables if t["connection"] == "test_db"]
        assert len(remote_tables) == 1
        assert remote_tables[0]["provider"] == "sqlite"


@pytest.mark.anyio
async def test_schema_tables_stream_includes_provider():
    """Schema tables stream endpoint includes provider field for remote connections."""
    # Create a SQLite in-memory engine with a table
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER, name TEXT)"))

    registry = ConnectionRegistry()
    # Register with explicit provider
    registry.register("test_db", lambda _req: engine, provider="sqlite")

    app, session_mgr = create_test_app(registry)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        # Upload a local table
        csv_content = b"x,y,label\n1,10,a\n2,20,b"
        files = {"file": ("data.csv", io.BytesIO(csv_content), "text/csv")}
        await client.post(f"/sessions/{session.id}/upload", files=files)

        # Request streaming table names
        response = await client.get(f"/sessions/{session.id}/schema/tables?stream=true")

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/x-ndjson"

        # Parse NDJSON lines
        lines = response.text.strip().split("\n")
        assert len(lines) == 1

        first_line = json.loads(lines[0])
        tables = first_line["tables"]

        # Local table should have provider=None
        local_tables = [t for t in tables if t["connection"] is None]
        assert len(local_tables) == 1
        assert local_tables[0]["provider"] is None

        # Remote table should have provider="sqlite"
        remote_tables = [t for t in tables if t["connection"] == "test_db"]
        assert len(remote_tables) == 1
        assert remote_tables[0]["provider"] == "sqlite"
