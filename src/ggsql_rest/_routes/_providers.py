from fastapi import APIRouter, Depends, Request

from .._connections import ConnectionRegistry
from .._models import ProviderInfo
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
