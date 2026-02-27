"""Tests for the /providers endpoint."""

import io
import json
import pytest
from unittest.mock import MagicMock
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from ggsql_rest import ConnectionRegistry
from ggsql_rest._errors import register_error_handlers
from ggsql_rest._sessions import SessionManager
from ggsql_rest._routes._sessions import router as sessions_router
from ggsql_rest._routes._sessions import get_session_manager
from ggsql_rest._routes._providers import router as providers_router
from ggsql_rest._routes._dependencies import (
    get_registry,
    get_snowflake_discovery,
    get_pins_discovery,
)


def create_test_app(
    registry: ConnectionRegistry | None = None,
    snowflake=None,
    pins=None,
    seed_data=None,
):
    """Create a test app with optional providers."""
    registry = registry or ConnectionRegistry()
    session_mgr = SessionManager(timeout_mins=30, seed_data=seed_data)

    app = FastAPI()
    app.dependency_overrides[get_session_manager] = lambda: session_mgr
    app.dependency_overrides[get_registry] = lambda: registry
    if snowflake is not None:
        app.dependency_overrides[get_snowflake_discovery] = lambda: snowflake
    if pins is not None:
        app.dependency_overrides[get_pins_discovery] = lambda: pins

    app.include_router(sessions_router)
    app.include_router(providers_router)
    register_error_handlers(app)

    return app, session_mgr


@pytest.mark.anyio
async def test_providers_duckdb_only():
    """With no external providers, only duckdb is listed."""
    app, session_mgr = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()
        resp = await client.get(f"/sessions/{session.id}/providers")
        assert resp.status_code == 200
        providers = resp.json()
        assert len(providers) == 1
        assert providers[0]["name"] == "duckdb"
        assert providers[0]["requiresAuth"] is False


@pytest.mark.anyio
async def test_providers_with_connections():
    """YAML-configured connections appear as a provider."""
    from sqlalchemy import create_engine
    from sqlalchemy.pool import StaticPool

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    registry = ConnectionRegistry()
    registry.register("test_db", lambda _req: engine, provider="sqlite")

    app, session_mgr = create_test_app(registry=registry)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()
        resp = await client.get(f"/sessions/{session.id}/providers")
        providers = resp.json()
        names = [p["name"] for p in providers]
        assert "duckdb" in names
        assert "connections" in names
        conn_provider = next(p for p in providers if p["name"] == "connections")
        assert conn_provider["requiresAuth"] is False


@pytest.mark.anyio
async def test_providers_with_snowflake():
    """Snowflake appears as requires_auth=true when configured."""
    mock_snowflake = MagicMock()
    app, session_mgr = create_test_app(snowflake=mock_snowflake)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()
        resp = await client.get(f"/sessions/{session.id}/providers")
        providers = resp.json()
        sf = next(p for p in providers if p["name"] == "snowflake")
        assert sf["requiresAuth"] is True
        assert sf["label"] == "Snowflake"


@pytest.mark.anyio
async def test_providers_with_pins():
    """Pins appears as requires_auth=true when configured."""
    mock_pins = MagicMock()
    app, session_mgr = create_test_app(pins=mock_pins)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()
        resp = await client.get(f"/sessions/{session.id}/providers")
        providers = resp.json()
        pins_entry = next(p for p in providers if p["name"] == "pins")
        assert pins_entry["requiresAuth"] is True
        assert pins_entry["label"] == "Posit Pins"


@pytest.mark.anyio
async def test_providers_all_enabled():
    """All providers appear when fully configured."""
    from sqlalchemy import create_engine
    from sqlalchemy.pool import StaticPool

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    registry = ConnectionRegistry()
    registry.register("test_db", lambda _req: engine, provider="sqlite")

    app, session_mgr = create_test_app(
        registry=registry,
        snowflake=MagicMock(),
        pins=MagicMock(),
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()
        resp = await client.get(f"/sessions/{session.id}/providers")
        providers = resp.json()
        names = [p["name"] for p in providers]
        assert names == ["duckdb", "connections", "snowflake", "pins"]


@pytest.mark.anyio
async def test_provider_tables_duckdb():
    """DuckDB provider returns local tables as NDJSON."""
    app, session_mgr = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        # Upload a file to create a local table
        csv_content = b"x,y\n1,2\n3,4"
        files = {"file": ("data.csv", io.BytesIO(csv_content), "text/csv")}
        await client.post(f"/sessions/{session.id}/upload", files=files)

        resp = await client.get(
            f"/sessions/{session.id}/providers/duckdb/tables"
        )
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/x-ndjson"

        lines = resp.text.strip().split("\n")
        assert len(lines) >= 1
        batch = json.loads(lines[0])
        table_names = [t["tableName"] for t in batch["tables"]]
        assert "data" in table_names


@pytest.mark.anyio
async def test_provider_tables_connections():
    """Connections provider returns remote tables as NDJSON."""
    from sqlalchemy import create_engine
    from sqlalchemy.pool import StaticPool

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER, name TEXT)"))

    registry = ConnectionRegistry()
    registry.register("test_db", lambda _req: engine, provider="sqlite")

    app, session_mgr = create_test_app(registry=registry)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()
        resp = await client.get(
            f"/sessions/{session.id}/providers/connections/tables"
        )
        assert resp.status_code == 200

        lines = resp.text.strip().split("\n")
        all_tables = []
        for line in lines:
            batch = json.loads(line)
            all_tables.extend(batch["tables"])

        assert any(t["tableName"] == "users" for t in all_tables)
        assert all(t["source"] == "test_db" for t in all_tables)


@pytest.mark.anyio
async def test_provider_tables_snowflake():
    """Snowflake provider streams tables from SnowflakeDiscovery."""
    mock_snowflake = MagicMock()
    mock_snowflake.stream_table_names.return_value = iter([
        ("DB1", [("USERS", "DB1.PUBLIC"), ("ORDERS", "DB1.PUBLIC")]),
    ])

    app, session_mgr = create_test_app(snowflake=mock_snowflake)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()
        resp = await client.get(
            f"/sessions/{session.id}/providers/snowflake/tables"
        )
        assert resp.status_code == 200

        lines = resp.text.strip().split("\n")
        batch = json.loads(lines[0])
        assert len(batch["tables"]) == 2
        assert all(t["provider"] == "snowflake" for t in batch["tables"])


@pytest.mark.anyio
async def test_provider_tables_pins():
    """Pins provider streams tables from PinsDiscovery."""
    mock_pins = MagicMock()
    mock_pins.stream_table_names.return_value = iter([
        ("carson", ["carson__sales_data", "carson__test_metrics"]),
    ])

    app, session_mgr = create_test_app(pins=mock_pins)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()
        resp = await client.get(
            f"/sessions/{session.id}/providers/pins/tables"
        )
        assert resp.status_code == 200

        lines = resp.text.strip().split("\n")
        batch = json.loads(lines[0])
        assert len(batch["tables"]) == 2
        assert all(t["provider"] == "pins" for t in batch["tables"])


@pytest.mark.anyio
async def test_provider_tables_unknown_provider():
    """Unknown provider returns 404."""
    app, session_mgr = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()
        resp = await client.get(
            f"/sessions/{session.id}/providers/nonexistent/tables"
        )
        assert resp.status_code == 404


@pytest.mark.anyio
async def test_provider_tables_unconfigured_snowflake():
    """Snowflake tables returns 404 when Snowflake is not configured."""
    app, session_mgr = create_test_app()  # no snowflake
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()
        resp = await client.get(
            f"/sessions/{session.id}/providers/snowflake/tables"
        )
        assert resp.status_code == 404


@pytest.mark.anyio
async def test_provider_tables_unconfigured_pins():
    """Pins tables returns 404 when Pins is not configured."""
    app, session_mgr = create_test_app()  # no pins
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()
        resp = await client.get(
            f"/sessions/{session.id}/providers/pins/tables"
        )
        assert resp.status_code == 404
