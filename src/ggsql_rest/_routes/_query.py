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
from .._pins import PinsDiscovery
from .._query import execute_ggsql, execute_sql
from ._sessions import get_session
from ._dependencies import get_registry, get_snowflake_discovery, get_pins_discovery

router = APIRouter(prefix="/sessions/{session_id}", tags=["query"])


def _resolve_engine(
    source: str,
    request: Request,
    registry: ConnectionRegistry,
    snowflake: SnowflakeDiscovery | None,
) -> Engine:
    """Resolve source name to engine via registry or Snowflake discovery."""
    if source in registry.list_connections():
        return registry.get_engine(source, request)
    if snowflake is not None and snowflake.has_connection(source, request):
        return snowflake.get_engine(source, request)
    raise KeyError(f"Unknown source: '{source}'")


@router.post("/query")
async def query(
    request: Request,
    body: QueryRequest,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
    snowflake: SnowflakeDiscovery | None = Depends(get_snowflake_discovery),
    pins: PinsDiscovery | None = Depends(get_pins_discovery),
) -> dict:
    """Execute a ggsql query."""
    engine = None
    if body.source:
        if pins is not None and pins.has_any_pin_for_query(body.query, request):
            pins.load_pins_for_query(body.query, request, session)
        else:
            engine = _resolve_engine(body.source, request, registry, snowflake)

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
    pins: PinsDiscovery | None = Depends(get_pins_discovery),
) -> dict:
    """Execute a pure SQL query."""
    engine = None
    if body.source:
        if pins is not None and pins.has_any_pin_for_query(body.query, request):
            pins.load_pins_for_query(body.query, request, session)
        else:
            engine = _resolve_engine(body.source, request, registry, snowflake)

    result = execute_sql(body.query, session, engine)

    return success_envelope(SqlResponse(**result))
