"""CLI entry point for ggsql-rest server."""

import argparse

import uvicorn

from ._app import create_app
from ._config import load_connections_from_yaml
from ._connections import ConnectionRegistry


def main() -> None:
    parser = argparse.ArgumentParser(description="ggsql REST API server")
    parser.add_argument(
        "--connections",
        help="Path to connections YAML config file",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to listen on (default: 8000)",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind to (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--cors-origins",
        nargs="*",
        help="Allowed CORS origins",
    )
    args = parser.parse_args()

    if args.connections:
        registry = load_connections_from_yaml(args.connections)
    else:
        registry = ConnectionRegistry()

    app = create_app(registry, cors_origins=args.cors_origins)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
