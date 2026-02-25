from collections import OrderedDict
from typing import Callable

from fastapi import Request
from sqlalchemy import Engine


class ConnectionRegistry:
    def __init__(self, max_engines: int = 100):
        self._factories: dict[str, Callable[[Request], Engine]] = {}
        self._providers: dict[str, str] = {}
        self._engines: OrderedDict[tuple[str, str], Engine] = OrderedDict()
        self._max_engines = max_engines

    def register(self, name: str, factory: Callable[[Request], Engine], provider: str | None = None) -> None:
        self._factories[name] = factory
        if provider is not None:
            self._providers[name] = provider

    def get_engine(self, name: str, request: Request) -> Engine:
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
        return request.headers.get("X-User-Id", "anonymous")

    def list_connections(self) -> list[str]:
        return list(self._factories.keys())

    def has_connection(self, name: str) -> bool:
        return name in self._factories

    def get_provider(self, name: str) -> str | None:
        return self._providers.get(name)

    def dispose_all(self) -> None:
        for engine in self._engines.values():
            engine.dispose()
        self._engines.clear()
