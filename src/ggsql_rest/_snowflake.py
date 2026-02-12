"""Snowflake catalog discovery with per-user OAuth authentication.

Discovers all databases, schemas, and tables a user has access to in Snowflake.
Supports two auth modes:
- Connect: OAuth via Posit-Connect-User-Session-Token header
- Local: ~/.snowflake/connections.toml via SNOWFLAKE_CONNECTION_NAME env var
"""

from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING

import snowflake.connector as snowflake_connector
from sqlalchemy import create_engine

from ._schema import get_remote_table_schemas

if TYPE_CHECKING:
    from fastapi import Request
    from snowflake.connector import SnowflakeConnection
    from sqlalchemy import Engine

    from ._models import TableSchema

# Optional import — only available on Connect
try:
    from posit.connect.external.snowflake import PositAuthenticator
except ImportError:
    PositAuthenticator = None  # type: ignore[assignment, misc]

_SESSION_TOKEN_HEADER = "posit-connect-user-session-token"


class SnowflakeDiscovery:
    """Discovers Snowflake catalog and provides per-user engines.

    Args:
        account: Snowflake account identifier.
        warehouse: Default warehouse for queries.
        connection_name: Optional name in ~/.snowflake/connections.toml (local dev).
    """

    def __init__(
        self,
        account: str,
        warehouse: str,
        connection_name: str | None = None,
    ):
        self.account = account
        self.warehouse = warehouse
        self.connection_name = connection_name

        # Per-user caches: user_id -> discovered connections
        self._discovered_connections: dict[str, dict[str, tuple[str, str]]] = {}
        # Engine cache: (user_id, connection_name) -> Engine
        self._engines: OrderedDict[tuple[str, str], Engine] = OrderedDict()
        self._max_engines = 50

    def _create_connection(
        self,
        request: Request,
        database: str | None = None,
        schema: str | None = None,
    ) -> SnowflakeConnection:
        """Create a Snowflake connection for the requesting user.

        On Connect: uses OAuth via Posit-Connect-User-Session-Token header.
        Locally: uses connection_name from ~/.snowflake/connections.toml.
        """
        kwargs: dict = {"warehouse": self.warehouse}
        if database:
            kwargs["database"] = database
        if schema:
            kwargs["schema"] = schema

        session_token = request.headers.get(_SESSION_TOKEN_HEADER)

        if session_token:
            # Connect mode: OAuth token exchange
            if PositAuthenticator is None:
                raise ImportError(
                    "posit-sdk is required for Connect OAuth. "
                    "Install with: pip install ggsql-rest[snowflake]"
                )
            auth = PositAuthenticator(
                local_authenticator="EXTERNALBROWSER",
                user_session_token=session_token,
            )
            kwargs["account"] = self.account
            kwargs["authenticator"] = auth.authenticator
            kwargs["token"] = auth.token
        elif self.connection_name:
            # Local mode: connections.toml
            kwargs["connection_name"] = self.connection_name
        else:
            raise ValueError(
                "Snowflake authentication requires either a "
                "Posit-Connect-User-Session-Token header (on Connect) "
                "or SNOWFLAKE_CONNECTION_NAME env var (local dev)."
            )

        return snowflake_connector.connect(**kwargs)

    def _discover_catalog(
        self,
        conn: SnowflakeConnection,
    ) -> list[tuple[str, str, str, str]]:
        """Discover all accessible databases, schemas, and tables.

        Returns list of (connection_name, database, schema, table_name) tuples.
        Skips INFORMATION_SCHEMA and databases that error on access.
        """
        results: list[tuple[str, str, str, str]] = []
        cursor = conn.cursor()

        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()

        for db_row in databases:
            db_name = db_row[1]  # name is at index 1 in SHOW DATABASES output

            try:
                cursor.execute(f'SHOW SCHEMAS IN DATABASE "{db_name}"')
                schemas = cursor.fetchall()
            except Exception:
                continue  # Skip inaccessible databases

            for schema_row in schemas:
                schema_name = schema_row[1]

                if schema_name == "INFORMATION_SCHEMA":
                    continue

                conn_name = f"{db_name}.{schema_name}"

                try:
                    cursor.execute(f'SHOW TABLES IN SCHEMA "{db_name}"."{schema_name}"')
                    tables = cursor.fetchall()
                except Exception:
                    continue  # Skip inaccessible schemas

                for table_row in tables:
                    table_name = table_row[1]
                    results.append((conn_name, db_name, schema_name, table_name))

        return results

    def _extract_user_id(self, request: Request) -> str:
        """Extract user ID from request headers."""
        return request.headers.get("x-user-id", "anonymous")

    def _create_engine(
        self,
        request: Request,
        database: str,
        schema: str,
    ) -> Engine:
        """Create a SQLAlchemy engine using snowflake.connector for auth."""
        def creator():
            return self._create_connection(request, database=database, schema=schema)
        return create_engine("snowflake://not@used/db", creator=creator)

    def _get_cached_engine(
        self,
        user_id: str,
        connection_name: str,
        request: Request,
        database: str,
        schema: str,
    ) -> Engine:
        """Get or create a cached engine with LRU eviction."""
        cache_key = (user_id, connection_name)

        # Check if already cached
        if cache_key in self._engines:
            # Move to end (most recently used)
            self._engines.move_to_end(cache_key)
            return self._engines[cache_key]

        # Create new engine
        engine = self._create_engine(request, database, schema)
        self._engines[cache_key] = engine

        # Evict oldest if cache is full
        if len(self._engines) > self._max_engines:
            _, oldest_engine = self._engines.popitem(last=False)
            oldest_engine.dispose()

        return engine

    def get_tables(
        self,
        request: Request,
        include_stats: bool,
    ) -> list[TableSchema]:
        """Get table schemas for all connections the user has access to.

        This is the main public API for the schema route.

        Note: include_stats is accepted for interface compatibility but always
        forced to False for Snowflake. Column stats queries (MIN/MAX/DISTINCT)
        are too expensive on large Snowflake tables and hit case-sensitivity
        issues with the snowflake-sqlalchemy dialect's lowercased identifiers.
        """
        user_id = self._extract_user_id(request)

        # Discover catalog if not cached for this user
        if user_id not in self._discovered_connections:
            conn = self._create_connection(request)
            catalog_data = self._discover_catalog(conn)
            conn.close()

            # Build connections dict: connection_name -> (database, schema)
            connections: dict[str, tuple[str, str]] = {}
            for conn_name, database, schema, _table_name in catalog_data:
                if conn_name not in connections:
                    connections[conn_name] = (database, schema)

            self._discovered_connections[user_id] = connections

        # Get schemas for all discovered connections
        # Always skip stats for Snowflake — too expensive and quoting issues.
        all_tables: list[TableSchema] = []
        connections = self._discovered_connections[user_id]

        for conn_name, (database, schema) in connections.items():
            engine = self._get_cached_engine(user_id, conn_name, request, database, schema)
            tables = get_remote_table_schemas(engine, conn_name, include_stats=False)
            all_tables.extend(tables)

        return all_tables

    def get_engine(
        self,
        connection_name: str,
        request: Request,
    ) -> Engine:
        """Get engine for a specific connection.

        This is used by the query route.

        Raises:
            KeyError: If connection_name is not found for this user.
        """
        user_id = self._extract_user_id(request)

        # Look up connection in user's discovered connections
        if user_id not in self._discovered_connections:
            raise KeyError(f"Connection '{connection_name}' not found")

        connections = self._discovered_connections[user_id]
        if connection_name not in connections:
            raise KeyError(f"Connection '{connection_name}' not found")

        database, schema = connections[connection_name]
        return self._get_cached_engine(user_id, connection_name, request, database, schema)

    def has_connection(
        self,
        connection_name: str,
        request: Request,
    ) -> bool:
        """Check if a connection belongs to this discovery."""
        user_id = self._extract_user_id(request)
        if user_id not in self._discovered_connections:
            return False
        return connection_name in self._discovered_connections[user_id]

    def dispose_all(self) -> None:
        """Dispose all cached engines and clear caches."""
        for engine in self._engines.values():
            engine.dispose()
        self._engines.clear()
        self._discovered_connections.clear()
