"""Integration tests for Snowflake discovery in routes."""

from fastapi import FastAPI

from ggsql_rest import ConnectionRegistry
from ggsql_rest._errors import register_error_handlers
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
