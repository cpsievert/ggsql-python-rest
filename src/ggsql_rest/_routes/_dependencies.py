"""Shared FastAPI dependencies for routes."""

from .._connections import ConnectionRegistry
from .._snowflake import SnowflakeDiscovery
from .._pins import PinsDiscovery


def get_registry() -> ConnectionRegistry:
    """Dependency placeholder — overridden by app factory."""
    raise RuntimeError("ConnectionRegistry not initialized")


def get_snowflake_discovery() -> SnowflakeDiscovery | None:
    """Dependency placeholder — overridden by app factory."""
    return None


def get_pins_discovery() -> PinsDiscovery | None:
    """Dependency placeholder — overridden by app factory."""
    return None
