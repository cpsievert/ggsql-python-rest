"""Schema introspection route."""

import json

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from .._connections import ConnectionRegistry
from .._models import SchemaResponse, TableNameEntry, TableNamesResponse, TableSchema, success_envelope
from .._schema import get_local_table_schema, get_remote_table_names, get_remote_table_schemas
from .._sessions import Session
from .._snowflake import SnowflakeDiscovery
from ._sessions import get_session
from ._dependencies import get_registry, get_snowflake_discovery

router = APIRouter(prefix="/sessions/{session_id}", tags=["schema"])


@router.get("/schema/tables", response_model=None)
async def schema_tables(
    request: Request,
    skip_snowflake: bool = False,
    stream: bool = False,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
    snowflake: SnowflakeDiscovery | None = Depends(get_snowflake_discovery),
) -> dict | StreamingResponse:
    """Return table names for all available tables (local + remote) without columns."""
    # Local + remote tables (always instant)
    local_tables: list[TableNameEntry] = []

    # Local tables from session's DuckDB
    for table_name in session.tables:
        local_tables.append(TableNameEntry(table_name=table_name, connection=None))

    # Remote tables from each registered connection
    for conn_name in registry.list_connections():
        engine = registry.get_engine(conn_name, request)
        remote_table_names = get_remote_table_names(engine)
        for table_name in remote_table_names:
            local_tables.append(TableNameEntry(table_name=table_name, connection=conn_name))

    if not stream:
        # Original non-streaming path
        tables = list(local_tables)
        if snowflake is not None and not skip_snowflake:
            snowflake_table_names = snowflake.get_table_names(request)
            for table_name, connection_name in snowflake_table_names:
                tables.append(TableNameEntry(table_name=table_name, connection=connection_name))
        return success_envelope(TableNamesResponse(tables=tables))

    # Streaming path: NDJSON
    def generate():
        # First line: local + remote tables
        if local_tables:
            line = {"tables": [t.model_dump(by_alias=True) for t in local_tables]}
            yield json.dumps(line) + "\n"

        # Subsequent lines: Snowflake tables per-database
        if snowflake is not None and not skip_snowflake:
            for _db_name, batch in snowflake.stream_table_names(request):
                entries = [
                    TableNameEntry(table_name=tn, connection=cn)
                    for tn, cn in batch
                ]
                line = {"tables": [e.model_dump(by_alias=True) for e in entries]}
                yield json.dumps(line) + "\n"

    return StreamingResponse(generate(), media_type="application/x-ndjson")


@router.get("/schema")
async def schema(
    request: Request,
    include_stats: bool = False,
    skip_snowflake: bool = False,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
    snowflake: SnowflakeDiscovery | None = Depends(get_snowflake_discovery),
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

    # Snowflake tables (if configured and not skipped)
    if snowflake is not None and not skip_snowflake:
        snowflake_tables = snowflake.get_tables(request, include_stats)
        tables.extend(snowflake_tables)

    return success_envelope(SchemaResponse(tables=tables))


@router.get("/schema/table/{table_name}")
async def schema_table(
    request: Request,
    table_name: str,
    connection: str | None = None,
    include_stats: bool = False,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
    snowflake: SnowflakeDiscovery | None = Depends(get_snowflake_discovery),
) -> dict:
    """Return schema for a single table (local or remote)."""
    table_schema: TableSchema | None = None

    # Local table
    if connection is None:
        if table_name in session.tables:
            table_schema = get_local_table_schema(
                session.duckdb, table_name, include_stats
            )
        else:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    # Remote table from ConnectionRegistry
    elif registry.has_connection(connection):
        engine = registry.get_engine(connection, request)
        remote_tables = get_remote_table_schemas(engine, connection, include_stats)
        # Filter to requested table
        matching = [t for t in remote_tables if t.table_name == table_name]
        if matching:
            table_schema = matching[0]
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Table '{table_name}' not found in connection '{connection}'"
            )

    # Snowflake table
    elif snowflake is not None and snowflake.has_connection(connection, request):
        table_schema = snowflake.get_single_table_schema(
            request, table_name, connection
        )
        if table_schema is None:
            raise HTTPException(
                status_code=404,
                detail=f"Table '{table_name}' not found in Snowflake connection '{connection}'"
            )

    # Connection not found
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Connection '{connection}' not found"
        )

    return success_envelope(table_schema)
