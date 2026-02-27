import json

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from .._connections import ConnectionRegistry
from .._models import ProviderInfo, TableNameEntry
from .._schema import get_remote_table_names
from .._sessions import Session
from .._snowflake import SnowflakeDiscovery
from .._pins import PinsDiscovery
from ._sessions import get_session
from ._dependencies import get_registry, get_snowflake_discovery, get_pins_discovery

router = APIRouter(prefix="/sessions/{session_id}", tags=["providers"])


@router.get("/providers")
async def list_providers(
    request: Request,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
    snowflake: SnowflakeDiscovery | None = Depends(get_snowflake_discovery),
    pins: PinsDiscovery | None = Depends(get_pins_discovery),
) -> list[dict]:
    """List configured data source providers with metadata."""
    providers: list[ProviderInfo] = []

    # DuckDB is always available
    providers.append(ProviderInfo(name="duckdb", label="DuckDB", requires_auth=False))

    # YAML-configured connections
    if registry.list_connections():
        providers.append(
            ProviderInfo(name="connections", label="Database Connections", requires_auth=False)
        )

    # Snowflake (requires per-user OAuth)
    if snowflake is not None:
        providers.append(
            ProviderInfo(name="snowflake", label="Snowflake", requires_auth=True)
        )

    # Pins (requires per-user auth via Connect)
    if pins is not None:
        providers.append(
            ProviderInfo(name="pins", label="Posit Pins", requires_auth=True)
        )

    return [p.model_dump(by_alias=True) for p in providers]


def _sort_and_dump(entries: list[TableNameEntry]) -> str:
    entries.sort(key=lambda e: e.table_name)
    return (
        json.dumps({"tables": [e.model_dump(by_alias=True) for e in entries]})
        + "\n"
    )


@router.get("/providers/{provider_name}/tables", response_model=None)
async def provider_tables(
    provider_name: str,
    request: Request,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
    snowflake: SnowflakeDiscovery | None = Depends(get_snowflake_discovery),
    pins: PinsDiscovery | None = Depends(get_pins_discovery),
) -> StreamingResponse:
    """Stream table names (NDJSON) for a single provider."""

    if provider_name == "duckdb":
        def generate():
            entries = [
                TableNameEntry(table_name=name) for name in session.tables
            ]
            if entries:
                yield _sort_and_dump(entries)

    elif provider_name == "connections":
        if not registry.list_connections():
            raise HTTPException(status_code=404, detail="No connections configured")

        def generate():
            for conn_name in sorted(registry.list_connections()):
                engine = registry.get_engine(conn_name, request)
                provider = registry.get_provider(conn_name)
                entries = [
                    TableNameEntry(table_name=name, source=conn_name, provider=provider)
                    for name in get_remote_table_names(engine)
                ]
                if entries:
                    yield _sort_and_dump(entries)

    elif provider_name == "snowflake":
        if snowflake is None:
            raise HTTPException(status_code=404, detail="Snowflake not configured")

        def generate():
            for _db_name, batch in snowflake.stream_table_names(request):
                entries = [
                    TableNameEntry(table_name=tn, source=cn, provider="snowflake")
                    for tn, cn in batch
                ]
                if entries:
                    yield _sort_and_dump(entries)

    elif provider_name == "pins":
        if pins is None:
            raise HTTPException(status_code=404, detail="Pins not configured")

        def generate():
            for owner, table_names in pins.stream_table_names(request):
                entries = [
                    TableNameEntry(table_name=tn, source=owner, provider="pins")
                    for tn in table_names
                ]
                if entries:
                    yield _sort_and_dump(entries)

    else:
        raise HTTPException(status_code=404, detail=f"Unknown provider: {provider_name}")

    return StreamingResponse(generate(), media_type="application/x-ndjson")
