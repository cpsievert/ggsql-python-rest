"""Snowflake catalog discovery with per-user OAuth authentication.

Discovers all databases, schemas, and tables a user has access to in Snowflake.
Supports two auth modes:
- Connect: OAuth via Posit-Connect-User-Session-Token header
- Local: ~/.snowflake/connections.toml via SNOWFLAKE_CONNECTION_NAME env var
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import snowflake.connector as snowflake_connector

if TYPE_CHECKING:
    from fastapi import Request
    from snowflake.connector import SnowflakeConnection

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
