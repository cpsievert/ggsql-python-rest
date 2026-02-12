"""FastAPI application factory."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncGenerator

from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware

from ._connections import ConnectionRegistry
from ._sessions import SessionManager
from ._errors import register_error_handlers
from ._routes import _health, _sessions, _query, _schema

if TYPE_CHECKING:
    import polars as pl

    from ._snowflake import SnowflakeDiscovery


def _make_lifespan(
    registry: ConnectionRegistry,
    session_manager: SessionManager,
    snowflake: SnowflakeDiscovery | None = None,
):
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        """Application lifespan handler."""
        app.state.registry = registry
        app.state.session_manager = session_manager
        yield
        registry.dispose_all()
        if snowflake is not None:
            snowflake.dispose_all()

    return lifespan


def create_app(
    registry: ConnectionRegistry,
    session_timeout_mins: int = 30,
    cors_origins: list[str] | None = None,
    seed_data: list[tuple[str, pl.DataFrame]] | None = None,
    snowflake: SnowflakeDiscovery | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application."""
    session_manager = SessionManager(session_timeout_mins, seed_data=seed_data)

    app = FastAPI(
        title="ggsql REST API",
        description="REST API server for ggsql with SQLAlchemy backend support",
        lifespan=_make_lifespan(registry, session_manager, snowflake),
    )

    # Set up dependency overrides
    from ._routes._dependencies import get_registry, get_snowflake_discovery
    app.dependency_overrides[_sessions.get_session_manager] = lambda: session_manager
    app.dependency_overrides[get_registry] = lambda: registry
    if snowflake is not None:
        app.dependency_overrides[get_snowflake_discovery] = lambda: snowflake

    # CORS (consumer configurable)
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # Register error handlers
    register_error_handlers(app)

    # Create /api/v1 router and register sub-routes
    api_v1 = APIRouter(prefix="/api/v1")
    api_v1.include_router(_health.router)
    api_v1.include_router(_sessions.router)
    api_v1.include_router(_query.router)
    api_v1.include_router(_schema.router)

    # Mount the versioned API
    app.include_router(api_v1)

    return app
