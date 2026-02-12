"""Tests for Snowflake discovery module."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import Request

from ggsql_rest._snowflake import SnowflakeDiscovery


def _make_request(headers: dict[str, str] | None = None) -> Request:
    """Create a mock FastAPI Request with optional headers."""
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
    }
    return Request(scope)


class TestSnowflakeConnection:
    """Test Snowflake connection creation."""

    def test_connect_local_uses_connection_name(self):
        """Local mode uses SNOWFLAKE_CONNECTION_NAME from connections.toml."""
        discovery = SnowflakeDiscovery(
            account="test-account",
            warehouse="TEST_WH",
            connection_name="my_conn",
        )
        request = _make_request()

        with patch("ggsql_rest._snowflake.snowflake_connector") as mock_sf:
            mock_conn = MagicMock()
            mock_sf.connect.return_value = mock_conn

            conn = discovery._create_connection(request)

            mock_sf.connect.assert_called_once_with(
                connection_name="my_conn",
                warehouse="TEST_WH",
            )
            assert conn is mock_conn

    def test_connect_oauth_uses_session_token(self):
        """Connect mode uses OAuth via Posit-Connect-User-Session-Token."""
        discovery = SnowflakeDiscovery(
            account="test-account",
            warehouse="TEST_WH",
        )
        request = _make_request({
            "Posit-Connect-User-Session-Token": "test-token-123",
        })

        with (
            patch("ggsql_rest._snowflake.snowflake_connector") as mock_sf,
            patch("ggsql_rest._snowflake.PositAuthenticator") as mock_auth_cls,
        ):
            mock_auth = MagicMock()
            mock_auth.authenticator = "oauth"
            mock_auth.token = "sf-access-token-xyz"
            mock_auth_cls.return_value = mock_auth
            mock_conn = MagicMock()
            mock_sf.connect.return_value = mock_conn

            conn = discovery._create_connection(request)

            mock_auth_cls.assert_called_once_with(
                local_authenticator="EXTERNALBROWSER",
                user_session_token="test-token-123",
            )
            mock_sf.connect.assert_called_once_with(
                account="test-account",
                warehouse="TEST_WH",
                authenticator="oauth",
                token="sf-access-token-xyz",
            )
            assert conn is mock_conn

    def test_connect_no_token_no_connection_name_raises(self):
        """Raises if no session token and no connection_name configured."""
        discovery = SnowflakeDiscovery(
            account="test-account",
            warehouse="TEST_WH",
        )
        request = _make_request()  # No token header

        with pytest.raises(ValueError, match="Snowflake authentication"):
            discovery._create_connection(request)


class TestCatalogDiscovery:
    """Test Snowflake catalog discovery via SHOW commands."""

    def test_discovers_databases_schemas_tables(self):
        """Discovers databases, schemas, and tables via SHOW commands."""
        discovery = SnowflakeDiscovery(
            account="test-account",
            warehouse="TEST_WH",
        )

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Mock fetchall to return different results for each query
        mock_cursor.fetchall.side_effect = [
            # SHOW DATABASES
            [("created_on", "DB1", "owner", "comment", "options", "retention_time")],
            # SHOW SCHEMAS IN DATABASE "DB1"
            [
                ("created_on", "PUBLIC", "database", "owner", "comment", "options"),
                ("created_on", "INFORMATION_SCHEMA", "database", "owner", "comment", "options"),
            ],
            # SHOW TABLES IN SCHEMA "DB1"."PUBLIC"
            [
                ("created_on", "USERS", "database", "schema", "kind", "comment"),
                ("created_on", "ORDERS", "database", "schema", "kind", "comment"),
            ],
        ]

        result = discovery._discover_catalog(mock_conn)

        # Verify result structure
        assert len(result) == 2
        assert result[0] == ("DB1.PUBLIC", "DB1", "PUBLIC", "USERS")
        assert result[1] == ("DB1.PUBLIC", "DB1", "PUBLIC", "ORDERS")

    def test_skips_information_schema(self):
        """INFORMATION_SCHEMA schemas are excluded from results."""
        discovery = SnowflakeDiscovery(
            account="test-account",
            warehouse="TEST_WH",
        )

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Mock fetchall to return different results for each query
        mock_cursor.fetchall.side_effect = [
            # SHOW DATABASES
            [("created_on", "DB1", "owner", "comment", "options", "retention_time")],
            # SHOW SCHEMAS returns only INFORMATION_SCHEMA
            [("created_on", "INFORMATION_SCHEMA", "database", "owner", "comment", "options")],
        ]

        result = discovery._discover_catalog(mock_conn)

        # Should be empty - INFORMATION_SCHEMA filtered out
        assert result == []

    def test_skips_inaccessible_databases(self):
        """Inaccessible databases are skipped silently."""
        discovery = SnowflakeDiscovery(
            account="test-account",
            warehouse="TEST_WH",
        )

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Track which call we're on
        call_count = {"execute": 0, "fetchall": 0}

        def execute_side_effect(query):
            call_count["execute"] += 1
            if 'SHOW SCHEMAS IN DATABASE "DB2"' in query:
                raise Exception("Access denied to DB2")

        def fetchall_side_effect():
            call_count["fetchall"] += 1
            if call_count["fetchall"] == 1:
                # SHOW DATABASES
                return [
                    ("created_on", "DB1", "owner", "comment", "options", "retention_time"),
                    ("created_on", "DB2", "owner", "comment", "options", "retention_time"),
                ]
            elif call_count["fetchall"] == 2:
                # SHOW SCHEMAS IN DATABASE "DB1"
                return [("created_on", "PUBLIC", "database", "owner", "comment", "options")]
            elif call_count["fetchall"] == 3:
                # SHOW TABLES IN SCHEMA "DB1"."PUBLIC"
                return [("created_on", "USERS", "database", "schema", "kind", "comment")]
            raise ValueError(f"Unexpected fetchall call #{call_count['fetchall']}")

        mock_cursor.execute.side_effect = execute_side_effect
        mock_cursor.fetchall.side_effect = fetchall_side_effect

        result = discovery._discover_catalog(mock_conn)

        # Should only have DB1 results, DB2 skipped
        assert len(result) == 1
        assert result[0] == ("DB1.PUBLIC", "DB1", "PUBLIC", "USERS")

    def test_empty_account_returns_empty(self):
        """Empty account with no databases returns empty list."""
        discovery = SnowflakeDiscovery(
            account="test-account",
            warehouse="TEST_WH",
        )

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # SHOW DATABASES returns empty
        mock_cursor.fetchall.return_value = []

        result = discovery._discover_catalog(mock_conn)

        assert result == []
