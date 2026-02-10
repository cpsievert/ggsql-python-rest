"""FastAPI application factory."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware

from ._connections import ConnectionRegistry
from ._sessions import SessionManager
from ._errors import register_error_handlers
from ._routes import _health, _sessions, _query, _schema


def _make_lifespan(
    registry: ConnectionRegistry,
    session_manager: SessionManager,
):
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        """Application lifespan handler."""
        app.state.registry = registry
        app.state.session_manager = session_manager
        yield
        registry.dispose_all()

    return lifespan


def create_app(
    registry: ConnectionRegistry,
    session_timeout_mins: int = 30,
    cors_origins: list[str] | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application."""
    session_manager = SessionManager(session_timeout_mins)

    app = FastAPI(
        title="ggsql REST API",
        description="REST API server for ggsql with SQLAlchemy backend support",
        lifespan=_make_lifespan(registry, session_manager),
    )

    # Set up dependency overrides
    app.dependency_overrides[_sessions.get_session_manager] = lambda: session_manager
    app.dependency_overrides[_query.get_registry] = lambda: registry

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
