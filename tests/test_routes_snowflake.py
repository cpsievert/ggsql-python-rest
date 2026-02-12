"""Integration tests for Snowflake discovery in routes."""

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from ggsql_rest import ConnectionRegistry
from ggsql_rest._errors import register_error_handlers
from ggsql_rest._models import ColumnSchema, TableSchema
from ggsql_rest._sessions import SessionManager
from ggsql_rest._snowflake import SnowflakeDiscovery
from ggsql_rest._routes._sessions import router as sessions_router, get_session_manager
from ggsql_rest._routes._query import router as query_router
from ggsql_rest._routes._schema import router as schema_router
from ggsql_rest._routes._dependencies import get_registry, get_snowflake_discovery


def create_test_app_with_snowflake(
    registry: ConnectionRegistry,
    snowflake: SnowflakeDiscovery | None = None,
) -> tuple[FastAPI, SessionManager]:
    """Create test app with optional Snowflake discovery."""
    app = FastAPI()
    session_mgr = SessionManager(timeout_mins=30)
    app.dependency_overrides[get_session_manager] = lambda: session_mgr
    app.dependency_overrides[get_registry] = lambda: registry
    if snowflake is not None:
        app.dependency_overrides[get_snowflake_discovery] = lambda: snowflake
    app.include_router(sessions_router)
    app.include_router(query_router)
    app.include_router(schema_router)
    register_error_handlers(app)
    return app, session_mgr


@pytest.mark.anyio
async def test_schema_includes_snowflake_tables():
    """Schema route includes tables from SnowflakeDiscovery."""
    mock_snowflake = MagicMock(spec=SnowflakeDiscovery)
    mock_snowflake.get_tables.return_value = [
        TableSchema(
            table_name="USERS",
            connection="MY_DB.PUBLIC",
            columns=[
                ColumnSchema(column_name="ID", data_type="NUMBER"),
                ColumnSchema(column_name="NAME", data_type="VARCHAR"),
            ],
        ),
    ]
    app, session_mgr = create_test_app_with_snowflake(ConnectionRegistry(), mock_snowflake)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()
        response = await client.get(f"/sessions/{session.id}/schema")
        assert response.status_code == 200
        tables = response.json()["data"]["tables"]
        snowflake_tables = [t for t in tables if t["connection"] == "MY_DB.PUBLIC"]
        assert len(snowflake_tables) == 1
        assert snowflake_tables[0]["tableName"] == "USERS"


@pytest.mark.anyio
async def test_schema_works_without_snowflake():
    """Schema route works when Snowflake is not configured."""
    app, session_mgr = create_test_app_with_snowflake(ConnectionRegistry())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()
        response = await client.get(f"/sessions/{session.id}/schema")
        assert response.status_code == 200
        assert response.json()["data"]["tables"] == []


@pytest.mark.anyio
async def test_schema_skip_snowflake():
    """Schema endpoint with skip_snowflake=true excludes Snowflake tables."""
    mock_snowflake = MagicMock(spec=SnowflakeDiscovery)
    mock_snowflake.get_tables.return_value = [
        TableSchema(
            table_name="USERS",
            connection="MY_DB.PUBLIC",
            columns=[ColumnSchema(column_name="ID", data_type="NUMBER")],
        ),
    ]
    app, session_mgr = create_test_app_with_snowflake(ConnectionRegistry(), mock_snowflake)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()
        response = await client.get(
            f"/sessions/{session.id}/schema?skip_snowflake=true"
        )
        assert response.status_code == 200
        tables = response.json()["data"]["tables"]
        assert len(tables) == 0  # No local tables, Snowflake skipped
        mock_snowflake.get_tables.assert_not_called()
