from typing import Any

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


def resolve_source(
    source: str,
    request: Request,
    registry: ConnectionRegistry,
    snowflake: SnowflakeDiscovery | None,
) -> tuple[Engine, Any | None]:
    """Resolve a source name to (engine, optional_adbc_conn)."""
    if source in registry.list_connections():
        return registry.get_engine(source, request), None
    if snowflake is not None and snowflake.has_connection(source, request):
        engine = snowflake.get_engine(source, request)
        adbc_conn = snowflake.get_adbc_connection(source, request)
        return engine, adbc_conn
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
    engine = None
    adbc_conn = None
    if body.source:
        if body.provider == "pins" and pins is not None:
            pins.load_pins_for_query(body.query, request, session)
        else:
            engine, adbc_conn = resolve_source(
                body.source, request, registry, snowflake
            )

    result = execute_ggsql(
        body.query, session, engine, max_rows=body.max_rows, adbc_conn=adbc_conn
    )

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
    engine = None
    adbc_conn = None
    if body.source:
        if body.provider == "pins" and pins is not None:
            pins.load_pins_for_query(body.query, request, session)
        else:
            engine, adbc_conn = resolve_source(
                body.source, request, registry, snowflake
            )

    result = execute_sql(
        body.query,
        session,
        engine,
        timeout_seconds=body.timeout_seconds,
        adbc_conn=adbc_conn,
    )

    return success_envelope(SqlResponse(**result))
