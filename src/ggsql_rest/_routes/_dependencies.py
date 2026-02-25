from .._connections import ConnectionRegistry
from .._snowflake import SnowflakeDiscovery
from .._pins import PinsDiscovery


def get_registry() -> ConnectionRegistry:
    """Placeholder — overridden by app factory."""
    raise RuntimeError("ConnectionRegistry not initialized")


def get_snowflake_discovery() -> SnowflakeDiscovery | None:
    """Placeholder — overridden by app factory."""
    return None


def get_pins_discovery() -> PinsDiscovery | None:
    """Placeholder — overridden by app factory."""
    return None
