"""Connection registry for named database connections."""

from collections import OrderedDict
from typing import Callable

from fastapi import Request
from sqlalchemy import Engine


class ConnectionRegistry:
    """Registry for named database connections with request-aware factories."""

    def __init__(self, max_engines: int = 100):
        self._factories: dict[str, Callable[[Request], Engine]] = {}
        self._providers: dict[str, str] = {}
        self._engines: OrderedDict[tuple[str, str], Engine] = OrderedDict()
        self._max_engines = max_engines

    def register(self, name: str, factory: Callable[[Request], Engine], provider: str | None = None) -> None:
        """Register a named connection factory.

        Args:
            name: Connection name
            factory: Factory function to create an engine
            provider: Optional provider type (e.g., "postgresql", "mysql", "sqlite")
        """
        self._factories[name] = factory
        if provider is not None:
            self._providers[name] = provider

    def get_engine(self, name: str, request: Request) -> Engine:
        """Get or create a cached engine by name and user."""
        if name not in self._factories:
            raise KeyError(f"Unknown connection: '{name}'")

        user_id = self._extract_user_id(request)
        cache_key = (name, user_id)

        if cache_key in self._engines:
            self._engines.move_to_end(cache_key)
            return self._engines[cache_key]

        engine = self._factories[name](request)
        self._engines[cache_key] = engine

        if len(self._engines) > self._max_engines:
            _, evicted = self._engines.popitem(last=False)
            evicted.dispose()

        return engine

    def _extract_user_id(self, request: Request) -> str:
        """Extract user ID from request headers."""
        return request.headers.get("X-User-Id", "anonymous")

    def list_connections(self) -> list[str]:
        """List available connection names."""
        return list(self._factories.keys())

    def has_connection(self, name: str) -> bool:
        """Check if a connection name is registered."""
        return name in self._factories

    def get_provider(self, name: str) -> str | None:
        """Get the provider type for a connection, or None if unknown."""
        return self._providers.get(name)

    def dispose_all(self) -> None:
        """Dispose all cached engines. Called on shutdown."""
        for engine in self._engines.values():
            engine.dispose()
        self._engines.clear()
