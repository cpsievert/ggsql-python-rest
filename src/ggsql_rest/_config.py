"""YAML connection configuration loading."""

from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import create_engine

from ._connections import ConnectionRegistry


def _provider_from_url(url: str) -> str | None:
    """Extract database provider name from a SQLAlchemy URL scheme.

    Args:
        url: SQLAlchemy URL (e.g., "postgresql+psycopg2://...", "mysql://...")

    Returns:
        Provider name (e.g., "postgresql", "mysql", "sqlite") or None if not parseable
    """
    # SQLAlchemy URLs: "dialect+driver://..." or "dialect://..."
    scheme = url.split("://")[0] if "://" in url else None
    if scheme is None:
        return None
    dialect = scheme.split("+")[0]  # e.g. "postgresql+psycopg2" -> "postgresql"
    return dialect or None


def load_connections_from_yaml(path: str | Path) -> ConnectionRegistry:
    """Load a ConnectionRegistry from a YAML config file.

    Expected format:
        connections:
          name:
            url: "postgresql://..."
            pool_size: 5          # any create_engine kwarg
            connect_args:
              sslmode: require
    """
    with open(path) as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict) or "connections" not in config:
        raise ValueError("Config file must have a top-level 'connections' key")

    registry = ConnectionRegistry()

    for name, conn_config in config["connections"].items():
        conn_config = dict(conn_config)  # shallow copy to avoid mutating
        if "url" not in conn_config:
            raise ValueError(f"Connection '{name}' is missing required 'url' key")

        url = conn_config.pop("url")
        kwargs = conn_config

        def make_factory(u: str, k: dict[str, Any]):
            def factory(request):
                return create_engine(u, **k)
            return factory

        provider = _provider_from_url(url)
        registry.register(name, make_factory(url, kwargs), provider=provider)

    return registry
