"""YAML connection configuration loading."""

from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import create_engine

from ._connections import ConnectionRegistry


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

        registry.register(name, make_factory(url, kwargs))

    return registry
