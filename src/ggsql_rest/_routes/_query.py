"""Query execution routes."""

from fastapi import APIRouter, Depends, Request
from sqlalchemy import Engine

from .._models import (
    QueryRequest,
    QueryResponse,
    QueryMetadata,
    SqlRequest,
    SqlResponse,
    success_envelope,
)
from .._connections import ConnectionRegistry
from .._sessions import Session
from .._snowflake import SnowflakeDiscovery
from .._query import execute_ggsql, execute_sql
from ._sessions import get_session
from ._dependencies import get_registry, get_snowflake_discovery

router = APIRouter(prefix="/sessions/{session_id}", tags=["query"])


def _resolve_engine(
    connection: str,
    request: Request,
    registry: ConnectionRegistry,
    snowflake: SnowflakeDiscovery | None,
) -> Engine:
    """Resolve connection name to engine via registry or Snowflake discovery."""
    if connection in registry.list_connections():
        return registry.get_engine(connection, request)
    if snowflake is not None and snowflake.has_connection(connection, request):
        return snowflake.get_engine(connection, request)
    raise KeyError(f"Unknown connection: '{connection}'")


@router.post("/query")
async def query(
    request: Request,
    body: QueryRequest,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
    snowflake: SnowflakeDiscovery | None = Depends(get_snowflake_discovery),
) -> dict:
    """Execute a ggsql query."""
    engine = None
    if body.connection:
        engine = _resolve_engine(body.connection, request, registry, snowflake)

    result = execute_ggsql(body.query, session, engine)

    return success_envelope(
        QueryResponse(
            spec=result["spec"],
            metadata=QueryMetadata(**result["metadata"]),
        )
    )


@router.post("/sql")
async def sql(
    request: Request,
    body: SqlRequest,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
    snowflake: SnowflakeDiscovery | None = Depends(get_snowflake_discovery),
) -> dict:
    """Execute a pure SQL query."""
    engine = None
    if body.connection:
        engine = _resolve_engine(body.connection, request, registry, snowflake)

    result = execute_sql(body.query, session, engine)

    return success_envelope(SqlResponse(**result))
