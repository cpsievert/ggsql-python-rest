"""CLI entry point for ggsql-rest server."""

import argparse

import uvicorn

from ._app import create_app
from ._config import load_connections_from_yaml
from ._connections import ConnectionRegistry
from ._sessions import load_seed_data


def main() -> None:
    parser = argparse.ArgumentParser(description="ggsql REST API server")
    parser.add_argument(
        "--connections",
        help="Path to connections YAML config file",
    )
    parser.add_argument(
        "--load-data",
        action="append",
        dest="load_data",
        metavar="FILE",
        help="Load a data file (CSV, Parquet, JSON) into all sessions. Can be repeated.",
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

    seed_data = load_seed_data(args.load_data) if args.load_data else None
    if seed_data:
        tables = [name for name, _ in seed_data]
        print(f"Loaded {len(seed_data)} data file(s): {', '.join(tables)}")

    app = create_app(registry, cors_origins=args.cors_origins, seed_data=seed_data)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
