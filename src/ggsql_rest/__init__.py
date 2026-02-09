"""ggsql REST API server with SQLAlchemy backend support."""

from importlib.metadata import version

from ._app import create_app
from ._connections import ConnectionRegistry

__version__ = version("ggsql-rest")
__all__ = ["create_app", "ConnectionRegistry"]
