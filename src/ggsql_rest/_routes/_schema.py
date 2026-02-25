"""Schema introspection route."""

import json

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from .._connections import ConnectionRegistry
from .._models import ColumnSchema, SchemaResponse, TableNameEntry, TableNamesResponse, TableSchema, success_envelope
from .._schema import get_local_table_schema, get_remote_table_names, get_remote_table_schemas
from .._sessions import Session
from .._snowflake import SnowflakeDiscovery
from .._pins import PinsDiscovery
from ._sessions import get_session
from ._dependencies import get_registry, get_snowflake_discovery, get_pins_discovery

router = APIRouter(prefix="/sessions/{session_id}", tags=["schema"])


@router.get("/schema/tables", response_model=None)
async def schema_tables(
    request: Request,
    skip_slow_discovery: bool = False,
    stream: bool = False,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
    snowflake: SnowflakeDiscovery | None = Depends(get_snowflake_discovery),
    pins: PinsDiscovery | None = Depends(get_pins_discovery),
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
        provider = registry.get_provider(conn_name)
        for table_name in remote_table_names:
            local_tables.append(TableNameEntry(table_name=table_name, connection=conn_name, provider=provider))

    if not stream:
        # Original non-streaming path
        tables = list(local_tables)
        if snowflake is not None and not skip_slow_discovery:
            snowflake_table_names = snowflake.get_table_names(request)
            for table_name, connection_name in snowflake_table_names:
                tables.append(TableNameEntry(table_name=table_name, connection=connection_name, provider="snowflake"))
        if pins is not None and not skip_slow_discovery:
            for owner, table_names in pins.stream_table_names(request):
                for table_name in table_names:
                    tables.append(TableNameEntry(table_name=table_name, connection=owner, provider="pins"))
        return success_envelope(TableNamesResponse(tables=tables))

    # Streaming path: NDJSON
    def generate():
        # First line: local + remote tables
        if local_tables:
            line = {"tables": [t.model_dump(by_alias=True) for t in local_tables]}
            yield json.dumps(line) + "\n"

        # Subsequent lines: Snowflake tables per-database
        if snowflake is not None and not skip_slow_discovery:
            for _db_name, batch in snowflake.stream_table_names(request):
                entries = [
                    TableNameEntry(table_name=tn, connection=cn, provider="snowflake")
                    for tn, cn in batch
                ]
                line = {"tables": [e.model_dump(by_alias=True) for e in entries]}
                yield json.dumps(line) + "\n"

        # Pins tables (one batch per owner)
        if pins is not None:
            for owner, table_names in pins.stream_table_names(request):
                entries = [
                    TableNameEntry(table_name=tn, connection=owner, provider="pins")
                    for tn in table_names
                ]
                line = {"tables": [e.model_dump(by_alias=True) for e in entries]}
                yield json.dumps(line) + "\n"

    return StreamingResponse(generate(), media_type="application/x-ndjson")


@router.get("/schema")
async def schema(
    request: Request,
    include_stats: bool = False,
    skip_slow_discovery: bool = False,
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
    if snowflake is not None and not skip_slow_discovery:
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
    pins: PinsDiscovery | None = Depends(get_pins_discovery),
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

    # Pins table (connection is the owner name, e.g., "garrick")
    elif pins is not None and pins.has_pin(table_name, request):
        # Read schema metadata only (no full data download)
        columns = pins.get_pin_schema(table_name, request)
        if columns:
            table_schema = TableSchema(
                table_name=table_name,
                connection=connection,
                columns=[
                    ColumnSchema(column_name=col_name, data_type=col_type)
                    for col_name, col_type in columns
                ],
            )
        else:
            # Fallback: load the full pin into DuckDB
            pins.ensure_pin_loaded(session.id, table_name, request, session)
            table_schema = get_local_table_schema(
                session.duckdb, table_name, include_stats
            )

    # Connection not found
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Connection '{connection}' not found"
        )

    return success_envelope(table_schema)
