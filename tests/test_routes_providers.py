"""Tests for the /providers endpoint."""

import pytest
from unittest.mock import MagicMock
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

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
