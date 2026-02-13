"""Snowflake catalog discovery with per-user OAuth authentication.

Discovers all databases, schemas, and tables a user has access to in Snowflake.
Supports two auth modes:
- Connect: OAuth via Posit-Connect-User-Session-Token header
- Local: ~/.snowflake/connections.toml via SNOWFLAKE_CONNECTION_NAME env var
"""

from __future__ import annotations

import json
from collections import OrderedDict
from typing import TYPE_CHECKING

import snowflake.connector as snowflake_connector
from sqlalchemy import create_engine

if TYPE_CHECKING:
    from collections.abc import Iterator

    from fastapi import Request
    from snowflake.connector import SnowflakeConnection
    from sqlalchemy import Engine

    from ._models import ColumnSchema, TableSchema

# Optional import — only available on Connect
try:
    from posit.connect.external.snowflake import PositAuthenticator
except ImportError:
    PositAuthenticator = None  # type: ignore[assignment, misc]

_SESSION_TOKEN_HEADER = "posit-connect-user-session-token"


def _parse_snowflake_type(data_type_json: str) -> str:
    """Parse Snowflake's JSON data_type string into a readable type name.

    SHOW COLUMNS returns data_type as JSON, e.g.:
      {"type":"FIXED","precision":38,"scale":0,"nullable":true}
    """
    try:
        parsed = json.loads(data_type_json)
    except (json.JSONDecodeError, TypeError):
        return "VARCHAR"

    sf_type = parsed.get("type", "VARCHAR")

    if sf_type == "FIXED":
        precision = parsed.get("precision", 38)
        scale = parsed.get("scale", 0)
        return f"NUMBER({precision},{scale})"
    elif sf_type == "TEXT":
        return "VARCHAR"
    elif sf_type == "REAL":
        return "FLOAT"
    else:
        # DATE, BOOLEAN, TIMESTAMP_NTZ, TIMESTAMP_LTZ, TIMESTAMP_TZ,
        # TIME, BINARY, VARIANT, OBJECT, ARRAY — use as-is
        return sf_type


class SnowflakeDiscovery:
    """Discovers Snowflake catalog and provides per-user engines.

    Args:
        account: Snowflake account identifier.
        warehouse: Default warehouse for queries.
        connection_name: Optional name in ~/.snowflake/connections.toml (local dev).
        databases: Optional list of database names to discover. If None, discovers all.
    """

    def __init__(
        self,
        account: str,
        warehouse: str,
        connection_name: str | None = None,
        databases: list[str] | None = None,
    ):
        self.account = account
        self.warehouse = warehouse
        self.connection_name = connection_name
        self.databases = databases

        # Per-user caches: user_id -> discovered connections
        self._discovered_connections: dict[str, dict[str, tuple[str, str]]] = {}
        # Per-user caches: user_id -> discovered tables
        self._discovered_tables: dict[str, list[TableSchema]] = {}
        # Per-user catalog cache: user_id -> list of (conn_name, db, schema, table_name)
        self._discovered_catalog: dict[str, list[tuple[str, str, str, str]]] = {}
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

        if self.databases:
            # Use specified databases (skip SHOW DATABASES)
            db_names = self.databases
        else:
            cursor.execute("SHOW DATABASES")
            db_names = [row[1] for row in cursor.fetchall()]

        for db_name in db_names:

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

    def _discover_catalog_by_database(
        self,
        conn: SnowflakeConnection,
    ) -> Iterator[tuple[str, list[tuple[str, str, str, str]]]]:
        """Discover tables database-by-database, yielding after each.

        Yields (database_name, entries) tuples where entries are
        (connection_name, database, schema, table_name) tuples.
        """
        cursor = conn.cursor()

        if self.databases:
            db_names = self.databases
        else:
            cursor.execute("SHOW DATABASES")
            db_names = [row[1] for row in cursor.fetchall()]

        for db_name in db_names:
            db_entries: list[tuple[str, str, str, str]] = []

            try:
                cursor.execute(f'SHOW SCHEMAS IN DATABASE "{db_name}"')
                schemas = cursor.fetchall()
            except Exception:
                continue

            for schema_row in schemas:
                schema_name = schema_row[1]
                if schema_name == "INFORMATION_SCHEMA":
                    continue

                conn_name = f"{db_name}.{schema_name}"

                try:
                    cursor.execute(
                        f'SHOW TABLES IN SCHEMA "{db_name}"."{schema_name}"'
                    )
                    tables = cursor.fetchall()
                except Exception:
                    continue

                for table_row in tables:
                    table_name = table_row[1]
                    db_entries.append((conn_name, db_name, schema_name, table_name))

            if db_entries:
                yield db_name, db_entries

    def _discover_columns(
        self,
        conn: SnowflakeConnection,
        databases: list[str],
    ) -> dict[tuple[str, str, str], list[tuple[str, str]]]:
        """Discover columns for all tables using SHOW COLUMNS IN DATABASE.

        Args:
            conn: Active Snowflake connection.
            databases: List of database names to query.

        Returns:
            Dict mapping (database, schema, table) -> [(column_name, data_type), ...]
        """
        columns: dict[tuple[str, str, str], list[tuple[str, str]]] = {}
        cursor = conn.cursor()

        for db_name in databases:
            try:
                cursor.execute(f'SHOW COLUMNS IN DATABASE "{db_name}"')
                rows = cursor.fetchall()
            except Exception:
                continue

            for row in rows:
                table_name = row[0]
                schema_name = row[1]
                col_name = row[2]
                data_type_json = row[3]

                if schema_name == "INFORMATION_SCHEMA":
                    continue

                key = (db_name, schema_name, table_name)
                if key not in columns:
                    columns[key] = []
                columns[key].append((col_name, _parse_snowflake_type(data_type_json)))

        return columns

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

    def get_table_names(
        self,
        request: Request,
    ) -> list[tuple[str, str]]:
        """Get table names and connection names for all accessible tables.

        This is a fast path that skips column introspection. Used for the
        listTables LLM tool.

        Returns:
            List of (table_name, connection_name) tuples.
        """
        user_id = self._extract_user_id(request)

        # Return cached catalog if already discovered for this user
        if user_id in self._discovered_catalog:
            catalog_data = self._discovered_catalog[user_id]
        else:
            # Open a connection and discover catalog
            conn = self._create_connection(request)
            try:
                catalog_data = self._discover_catalog(conn)
            finally:
                conn.close()

            # Cache the catalog data
            self._discovered_catalog[user_id] = catalog_data

            # Also populate connections cache from catalog data
            connections: dict[str, tuple[str, str]] = {}
            for conn_name, database, schema, _table_name in catalog_data:
                if conn_name not in connections:
                    connections[conn_name] = (database, schema)
            self._discovered_connections[user_id] = connections

        # Build result list of (table_name, connection_name) tuples
        result: list[tuple[str, str]] = []
        for conn_name, _database, _schema, table_name in catalog_data:
            result.append((table_name, conn_name))

        return result

    def stream_table_names(
        self,
        request: Request,
    ) -> Iterator[tuple[str, list[tuple[str, str]]]]:
        """Stream table names per-database.

        Yields (database_name, table_entries) tuples where table_entries
        are (table_name, connection_name) pairs.

        Also populates the catalog and connections caches incrementally.
        """
        user_id = self._extract_user_id(request)

        # If already cached, yield everything at once (grouped by database)
        if user_id in self._discovered_catalog:
            catalog_data = self._discovered_catalog[user_id]
            by_db: dict[str, list[tuple[str, str]]] = {}
            for conn_name, database, _schema, table_name in catalog_data:
                by_db.setdefault(database, []).append((table_name, conn_name))
            for db_name, entries in by_db.items():
                yield db_name, entries
            return

        conn = self._create_connection(request)
        try:
            all_catalog: list[tuple[str, str, str, str]] = []
            connections: dict[str, tuple[str, str]] = {}

            for db_name, db_entries in self._discover_catalog_by_database(conn):
                batch: list[tuple[str, str]] = []
                for conn_name, database, schema, table_name in db_entries:
                    all_catalog.append((conn_name, database, schema, table_name))
                    if conn_name not in connections:
                        connections[conn_name] = (database, schema)
                    batch.append((table_name, conn_name))

                # Register connections before yielding so they're queryable
                # as soon as the client receives the table names
                self._discovered_connections[user_id] = dict(connections)

                yield db_name, batch

            # Cache full catalog after iteration completes
            self._discovered_catalog[user_id] = all_catalog
        finally:
            conn.close()

    def get_tables(
        self,
        request: Request,
        include_stats: bool,
    ) -> list[TableSchema]:
        """Get table schemas for all connections the user has access to.

        Uses SHOW COLUMNS IN DATABASE to get column metadata from the
        existing discovery connection, avoiding per-schema engine creation.
        """
        user_id = self._extract_user_id(request)

        # Return cached tables if already discovered for this user
        if user_id in self._discovered_tables:
            return self._discovered_tables[user_id]

        # Open a single connection for all discovery
        conn = self._create_connection(request)
        try:
            catalog_data = self._discover_catalog(conn)

            # Build connections dict and collect metadata
            connections: dict[str, tuple[str, str]] = {}
            databases: set[str] = set()
            table_connections: dict[tuple[str, str, str], str] = {}

            for conn_name, database, schema, table_name in catalog_data:
                if conn_name not in connections:
                    connections[conn_name] = (database, schema)
                databases.add(database)
                table_connections[(database, schema, table_name)] = conn_name

            # Get all columns via SHOW COLUMNS IN DATABASE
            all_columns = self._discover_columns(conn, sorted(databases))
        finally:
            conn.close()

        # Build TableSchema objects
        from ._models import ColumnSchema, TableSchema

        all_tables: list[TableSchema] = []
        for (db, schema, table), cols in all_columns.items():
            conn_name = table_connections.get((db, schema, table))
            if conn_name is None:
                continue  # Column for a table not in our catalog (e.g., views)

            all_tables.append(
                TableSchema(
                    table_name=table,
                    connection=conn_name,
                    columns=[
                        ColumnSchema(column_name=name, data_type=dtype)
                        for name, dtype in cols
                    ],
                )
            )

        # Cache results
        self._discovered_connections[user_id] = connections
        self._discovered_tables[user_id] = all_tables

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

    def get_single_table_schema(
        self,
        request: Request,
        table_name: str,
        connection: str,
    ) -> TableSchema | None:
        """Get column schema for a single Snowflake table.

        Uses SHOW COLUMNS IN TABLE for targeted introspection.

        Args:
            request: FastAPI request (for auth).
            table_name: The table name.
            connection: Connection name in "DATABASE.SCHEMA" format.

        Returns:
            TableSchema if found, None if connection or table not found.
        """
        from ._models import ColumnSchema, TableSchema

        user_id = self._extract_user_id(request)
        connections = self._discovered_connections.get(user_id, {})
        if connection not in connections:
            return None

        database, schema = connections[connection]

        conn = self._create_connection(request, database=database, schema=schema)
        try:
            cursor = conn.cursor()
            cursor.execute(
                f'SHOW COLUMNS IN TABLE "{database}"."{schema}"."{table_name}"'
            )
            rows = cursor.fetchall()

            if not rows:
                return None

            columns = [
                ColumnSchema(
                    column_name=row[2],
                    data_type=_parse_snowflake_type(row[3]),
                )
                for row in rows
            ]

            return TableSchema(
                table_name=table_name,
                connection=connection,
                columns=columns,
            )
        finally:
            conn.close()

    def dispose_all(self) -> None:
        """Dispose all cached engines and clear caches."""
        for engine in self._engines.values():
            engine.dispose()
        self._engines.clear()
        self._discovered_connections.clear()
        self._discovered_tables.clear()
        self._discovered_catalog.clear()
