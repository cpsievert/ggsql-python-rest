import json

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from .._connections import ConnectionRegistry
from .._models import (
    ColumnSchema,
    TableNameEntry,
    TableSchema,
    success_envelope,
)
from .._schema import (
    get_local_table_schema,
    get_remote_single_table_schema,
    get_remote_table_names,
)
from .._sessions import Session
from .._snowflake import SnowflakeDiscovery
from .._pins import PinsDiscovery
from ._sessions import get_session
from ._dependencies import get_registry, get_snowflake_discovery, get_pins_discovery

router = APIRouter(prefix="/sessions/{session_id}", tags=["schema"])


@router.get("/schema/tables", response_model=None)
async def schema_tables(
    request: Request,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
    snowflake: SnowflakeDiscovery | None = Depends(get_snowflake_discovery),
    pins: PinsDiscovery | None = Depends(get_pins_discovery),
) -> StreamingResponse:
    """Stream table names (NDJSON) for all sources.

    Discovery order: local DuckDB -> registered connections -> pins -> snowflake
    """

    def sort_and_dump(entries: list[TableNameEntry]):
        entries.sort(key=lambda e: e.table_name)
        return (
            json.dumps({"tables": [e.model_dump(by_alias=True) for e in entries]})
            + "\n"
        )

    def generate():
        local_tables = [
            TableNameEntry(table_name=name, source=None) for name in session.tables
        ]
        if local_tables:
            yield sort_and_dump(local_tables)

        for conn_name in sorted(registry.list_connections()):
            engine = registry.get_engine(conn_name, request)
            provider = registry.get_provider(conn_name)
            entries = [
                TableNameEntry(table_name=name, source=conn_name, provider=provider)
                for name in get_remote_table_names(engine)
            ]
            if entries:
                yield sort_and_dump(entries)

        if pins is not None:
            for owner, table_names in pins.stream_table_names(request):
                entries = [
                    TableNameEntry(table_name=tn, source=owner, provider="pins")
                    for tn in table_names
                ]
                yield sort_and_dump(entries)

        if snowflake is not None:
            for _db_name, batch in snowflake.stream_table_names(request):
                entries = [
                    TableNameEntry(table_name=tn, source=cn, provider="snowflake")
                    for tn, cn in batch
                ]
                yield sort_and_dump(entries)

    return StreamingResponse(generate(), media_type="application/x-ndjson")


@router.get("/schema/table/{table_name}")
async def schema_table(
    request: Request,
    table_name: str,
    source: str | None = None,
    include_stats: bool = False,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
    snowflake: SnowflakeDiscovery | None = Depends(get_snowflake_discovery),
    pins: PinsDiscovery | None = Depends(get_pins_discovery),
) -> dict:
    table_schema: TableSchema | None = None

    if source is None:
        if table_name in session.tables:
            table_schema = get_local_table_schema(
                session.duckdb, table_name, include_stats
            )
        else:
            raise HTTPException(
                status_code=404, detail=f"Table '{table_name}' not found"
            )

    elif registry.has_connection(source):
        engine = registry.get_engine(source, request)
        table_schema = get_remote_single_table_schema(
            engine, source, table_name, include_stats
        )
        if table_schema is None:
            raise HTTPException(
                status_code=404,
                detail=f"Table '{table_name}' not found in source '{source}'",
            )

    elif snowflake is not None and snowflake.has_connection(source, request):
        table_schema = snowflake.get_single_table_schema(request, table_name, source)
        if table_schema is None:
            raise HTTPException(
                status_code=404,
                detail=f"Table '{table_name}' not found in Snowflake source '{source}'",
            )

    elif pins is not None and pins.has_pin(table_name, request):
        columns = pins.get_pin_schema(table_name, request)
        if columns:
            table_schema = TableSchema(
                table_name=table_name,
                source=source,
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

    else:
        raise HTTPException(status_code=404, detail=f"Source '{source}' not found")

    return success_envelope(table_schema)
