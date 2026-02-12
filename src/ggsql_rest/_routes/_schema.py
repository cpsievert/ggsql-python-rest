"""Schema introspection route."""

from fastapi import APIRouter, Depends, Request

from .._connections import ConnectionRegistry
from .._models import SchemaResponse, TableNameEntry, TableNamesResponse, success_envelope
from .._schema import get_local_table_schema, get_remote_table_names, get_remote_table_schemas
from .._sessions import Session
from .._snowflake import SnowflakeDiscovery
from ._sessions import get_session
from ._dependencies import get_registry, get_snowflake_discovery

router = APIRouter(prefix="/sessions/{session_id}", tags=["schema"])


@router.get("/schema/tables")
async def schema_tables(
    request: Request,
    skip_snowflake: bool = False,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
    snowflake: SnowflakeDiscovery | None = Depends(get_snowflake_discovery),
) -> dict:
    """Return table names for all available tables (local + remote) without columns."""
    tables: list[TableNameEntry] = []

    # Local tables from session's DuckDB
    for table_name in session.tables:
        tables.append(TableNameEntry(table_name=table_name, connection=None))

    # Remote tables from each registered connection
    for conn_name in registry.list_connections():
        engine = registry.get_engine(conn_name, request)
        remote_table_names = get_remote_table_names(engine)
        for table_name in remote_table_names:
            tables.append(TableNameEntry(table_name=table_name, connection=conn_name))

    # Snowflake tables (if configured and not skipped)
    if snowflake is not None and not skip_snowflake:
        snowflake_table_names = snowflake.get_table_names(request)
        for table_name, connection_name in snowflake_table_names:
            tables.append(TableNameEntry(table_name=table_name, connection=connection_name))

    return success_envelope(TableNamesResponse(tables=tables))


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
