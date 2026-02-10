"""Schema introspection route."""

from fastapi import APIRouter, Depends, Request

from .._connections import ConnectionRegistry
from .._models import SchemaResponse, success_envelope
from .._schema import get_local_table_schema, get_remote_table_schemas
from .._sessions import Session
from ._sessions import get_session
from ._query import get_registry

router = APIRouter(prefix="/sessions/{session_id}", tags=["schema"])


@router.get("/schema")
async def schema(
    request: Request,
    include_stats: bool = False,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
) -> dict:
    """Return schema for all available tables (local + remote)."""
    tables = []

    # Local tables from session's DuckDB
    for table_name in session.tables:
        table_schema = get_local_table_schema(
            session.duckdb, table_name, include_stats
        )
        tables.append(table_schema)

    # Remote tables from each registered connection
    for conn_name in registry.list_connections():
        engine = registry.get_engine(conn_name, request)
        remote_tables = get_remote_table_schemas(engine, conn_name, include_stats)
        tables.extend(remote_tables)

    return success_envelope(SchemaResponse(tables=tables))
