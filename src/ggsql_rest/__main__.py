"""CLI entry point for ggsql-rest server."""

import argparse
import os

import uvicorn

from ._app import create_app
from ._config import load_connections_from_yaml
from ._connections import ConnectionRegistry
from ._sessions import load_seed_data, make_sample_data


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
        "--load-sample-data",
        action="store_true",
        dest="load_sample_data",
        help="Load sample data (products, sales, employees) into all sessions.",
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

    seed_data = load_seed_data(args.load_data) if args.load_data else []
    if args.load_sample_data:
        seed_data = make_sample_data() + seed_data
    if seed_data:
        tables = [name for name, _ in seed_data]
        print(f"Loaded {len(seed_data)} table(s): {', '.join(tables)}")
    seed_data = seed_data or None

    # Snowflake discovery via environment variables
    snowflake = None
    snowflake_account = os.environ.get("SNOWFLAKE_ACCOUNT")
    snowflake_warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")

    if snowflake_account and snowflake_warehouse:
        from ._snowflake import SnowflakeDiscovery

        snowflake = SnowflakeDiscovery(
            account=snowflake_account,
            warehouse=snowflake_warehouse,
            connection_name=os.environ.get("SNOWFLAKE_CONNECTION_NAME"),
        )
        print(f"Snowflake discovery enabled (account: {snowflake_account})")
    elif snowflake_account or snowflake_warehouse:
        print(
            "Warning: Both SNOWFLAKE_ACCOUNT and SNOWFLAKE_WAREHOUSE must be set "
            "to enable Snowflake discovery."
        )

    app = create_app(
        registry, cors_origins=args.cors_origins, seed_data=seed_data, snowflake=snowflake
    )
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
