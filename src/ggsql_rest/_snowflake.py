from __future__ import annotations

import json
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

import snowflake.connector as snowflake_connector
from sqlalchemy import create_engine

if TYPE_CHECKING:
    from collections.abc import Iterator

    from fastapi import Request
    from snowflake.connector import SnowflakeConnection
    from sqlalchemy import Engine

    from ._models import TableSchema

# Optional import — only available on Connect
try:
    from posit.connect.external.snowflake import PositAuthenticator
except ImportError:
    PositAuthenticator = None  # type: ignore[assignment, misc]

try:
    import adbc_driver_snowflake.dbapi as snowflake_adbc

    HAS_SNOWFLAKE_ADBC = True
except ImportError:
    snowflake_adbc = None  # type: ignore[assignment]
    HAS_SNOWFLAKE_ADBC = False

from ggsql_rest._constants import SESSION_TOKEN_HEADER


def parse_snowflake_type(data_type_json: str) -> str:
    """SHOW COLUMNS returns data_type as JSON, e.g.:
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

        self._discovered_connections: dict[str, dict[str, tuple[str, str]]] = {}
        self._discovered_catalog: dict[str, list[tuple[str, str, str, str]]] = {}
        self._engines: OrderedDict[tuple[str, str], Engine] = OrderedDict()
        self._max_engines = 50

    def _create_connection(
        self,
        request: Request,
        database: str | None = None,
        schema: str | None = None,
    ) -> SnowflakeConnection:
        """On Connect: uses OAuth via Posit-Connect-User-Session-Token header.
        Locally: uses connection_name from ~/.snowflake/connections.toml.
        """
        kwargs: dict = {"warehouse": self.warehouse}
        if database:
            kwargs["database"] = database
        if schema:
            kwargs["schema"] = schema

        session_token = request.headers.get(SESSION_TOKEN_HEADER)

        if session_token:
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
            kwargs["connection_name"] = self.connection_name
        else:
            raise ValueError(
                "Snowflake authentication requires either a "
                "Posit-Connect-User-Session-Token header (on Connect) "
                "or SNOWFLAKE_CONNECTION_NAME env var (local dev)."
            )

        return snowflake_connector.connect(**kwargs)

    def _discover_catalog_by_database(
        self,
        conn: SnowflakeConnection,
    ) -> Iterator[tuple[str, list[tuple[str, str, str, str]]]]:
        """Yields (database_name, entries) where entries are
        (connection_name, database, schema, table_name) tuples.
        """
        cursor = conn.cursor()

        if self.databases:
            db_names = self.databases
        else:
            cursor.execute("SHOW DATABASES")
            db_names = [row[1] for row in cursor.fetchall()]

        for db_name in sorted(db_names):
            db_entries: list[tuple[str, str, str, str]] = []

            try:
                cursor.execute(f'SHOW TABLES IN DATABASE "{db_name}"')
                tables = cursor.fetchall()
            except Exception:
                continue

            for table_row in tables:
                table_name = table_row[1]
                schema_name = table_row[3]
                if schema_name == "INFORMATION_SCHEMA":
                    continue
                conn_name = f"{db_name}.{schema_name}"
                db_entries.append((conn_name, db_name, schema_name, table_name))

            if db_entries:
                yield db_name, db_entries

    def _extract_user_id(self, request: Request) -> str:
        return request.headers.get("x-user-id", "anonymous")

    def _create_engine(
        self,
        request: Request,
        database: str,
        schema: str,
    ) -> Engine:
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
        cache_key = (user_id, connection_name)

        if cache_key in self._engines:
            self._engines.move_to_end(cache_key)
            return self._engines[cache_key]

        engine = self._create_engine(request, database, schema)
        self._engines[cache_key] = engine

        if len(self._engines) > self._max_engines:
            _, oldest_engine = self._engines.popitem(last=False)
            oldest_engine.dispose()

        return engine

    def stream_table_names(
        self,
        request: Request,
    ) -> Iterator[tuple[str, list[tuple[str, str]]]]:
        """Yields (database_name, table_entries) where table_entries
        are (table_name, connection_name) pairs.

        Also populates the catalog and connections caches incrementally.
        """
        user_id = self._extract_user_id(request)

        if user_id in self._discovered_catalog:
            catalog_data = self._discovered_catalog[user_id]
            by_db: dict[str, list[tuple[str, str]]] = {}
            for conn_name, database, _schema, table_name in catalog_data:
                by_db.setdefault(database, []).append((table_name, conn_name))
            for db_name, entries in sorted(by_db.items()):
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

            self._discovered_catalog[user_id] = all_catalog
        finally:
            conn.close()

    def get_engine(
        self,
        connection_name: str,
        request: Request,
    ) -> Engine:
        user_id = self._extract_user_id(request)

        if user_id not in self._discovered_connections:
            raise KeyError(f"Connection '{connection_name}' not found")

        connections = self._discovered_connections[user_id]
        if connection_name not in connections:
            raise KeyError(f"Connection '{connection_name}' not found")

        database, schema = connections[connection_name]
        return self._get_cached_engine(
            user_id, connection_name, request, database, schema
        )

    def has_connection(
        self,
        connection_name: str,
        request: Request,
    ) -> bool:
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
        """Uses SHOW COLUMNS IN TABLE for targeted introspection."""
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
                    data_type=parse_snowflake_type(row[3]),
                )
                for row in rows
            ]

            return TableSchema(
                table_name=table_name,
                source=connection,
                columns=columns,
            )
        finally:
            conn.close()

    def has_adbc_support(self) -> bool:
        """Check if ADBC Arrow Flight is available for Snowflake queries."""
        return HAS_SNOWFLAKE_ADBC

    def _build_adbc_params(
        self,
        token: str | None,
        database: str,
        schema: str,
    ) -> dict[str, str] | None:
        """Build ADBC connection parameters for Snowflake.

        Returns None if ADBC auth is not supported for the current auth mode
        (e.g., local dev using connections.toml).
        """
        if token is None:
            return None

        return {
            "adbc.snowflake.sql.account": self.account,
            "adbc.snowflake.sql.warehouse": self.warehouse,
            "adbc.snowflake.sql.db": database,
            "adbc.snowflake.sql.schema": schema,
            "adbc.snowflake.sql.auth_type": "auth_oauth",
            "adbc.snowflake.sql.client_option.auth_token": token,
        }

    def create_adbc_connection(
        self,
        request: Request,
        database: str,
        schema: str,
    ) -> Any | None:
        """Create an ADBC connection for Arrow-native Snowflake queries.

        Returns an ADBC DBAPI2 connection, or None if ADBC is unavailable
        or the auth mode doesn't support it.
        """
        if not HAS_SNOWFLAKE_ADBC or snowflake_adbc is None:
            return None

        session_token = request.headers.get(SESSION_TOKEN_HEADER)
        if session_token:
            if PositAuthenticator is None:
                return None
            auth = PositAuthenticator(
                local_authenticator="EXTERNALBROWSER",
                user_session_token=session_token,
            )
            token = auth.token
        else:
            return None

        params = self._build_adbc_params(token, database, schema)
        if params is None:
            return None

        return snowflake_adbc.connect(db_kwargs=params)

    def dispose_all(self) -> None:
        for engine in self._engines.values():
            engine.dispose()
        self._engines.clear()
        self._discovered_connections.clear()
        self._discovered_catalog.clear()
