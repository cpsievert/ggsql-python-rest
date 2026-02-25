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


class TestGetEngine:
    """Test get_engine() method for query route."""

    def test_get_engine_returns_engine_for_known_connection(self):
        """get_engine() returns engine for a known connection."""
        discovery = SnowflakeDiscovery(
            account="test-account",
            warehouse="TEST_WH",
            connection_name="my_conn",
        )
        request = _make_request({"x-user-id": "user1"})

        # Pre-populate discovered connections cache
        discovery._discovered_connections["user1"] = {
            "DB1.PUBLIC": ("DB1", "PUBLIC"),
        }

        mock_engine = MagicMock()
        with patch.object(discovery, "_create_engine", return_value=mock_engine):
            engine = discovery.get_engine("DB1.PUBLIC", request)

            assert engine is mock_engine
            discovery._create_engine.assert_called_once()

    def test_get_engine_unknown_connection_raises(self):
        """get_engine() raises KeyError for unknown connection."""
        discovery = SnowflakeDiscovery(
            account="test-account",
            warehouse="TEST_WH",
            connection_name="my_conn",
        )
        request = _make_request({"x-user-id": "user1"})

        # Pre-populate with empty discovered connections
        discovery._discovered_connections["user1"] = {}

        with pytest.raises(KeyError, match="DB1.PUBLIC"):
            discovery.get_engine("DB1.PUBLIC", request)


class TestParseSnowflakeType:
    """Test Snowflake JSON data_type parsing."""

    def test_fixed_type(self):
        from ggsql_rest._snowflake import _parse_snowflake_type
        assert _parse_snowflake_type('{"type":"FIXED","precision":38,"scale":0,"nullable":true}') == "NUMBER(38,0)"

    def test_fixed_with_scale(self):
        from ggsql_rest._snowflake import _parse_snowflake_type
        assert _parse_snowflake_type('{"type":"FIXED","precision":10,"scale":2,"nullable":true}') == "NUMBER(10,2)"

    def test_text_type(self):
        from ggsql_rest._snowflake import _parse_snowflake_type
        assert _parse_snowflake_type('{"type":"TEXT","length":16777216,"nullable":true,"fixed":false}') == "VARCHAR"

    def test_real_type(self):
        from ggsql_rest._snowflake import _parse_snowflake_type
        assert _parse_snowflake_type('{"type":"REAL","nullable":true}') == "FLOAT"

    def test_date_type(self):
        from ggsql_rest._snowflake import _parse_snowflake_type
        assert _parse_snowflake_type('{"type":"DATE","nullable":true}') == "DATE"

    def test_boolean_type(self):
        from ggsql_rest._snowflake import _parse_snowflake_type
        assert _parse_snowflake_type('{"type":"BOOLEAN","nullable":true}') == "BOOLEAN"

    def test_timestamp_types(self):
        from ggsql_rest._snowflake import _parse_snowflake_type
        assert _parse_snowflake_type('{"type":"TIMESTAMP_NTZ","precision":0,"scale":9,"nullable":true}') == "TIMESTAMP_NTZ"
        assert _parse_snowflake_type('{"type":"TIMESTAMP_LTZ","precision":0,"scale":9,"nullable":true}') == "TIMESTAMP_LTZ"

    def test_variant_type(self):
        from ggsql_rest._snowflake import _parse_snowflake_type
        assert _parse_snowflake_type('{"type":"VARIANT","nullable":true}') == "VARIANT"

    def test_invalid_json_returns_varchar(self):
        from ggsql_rest._snowflake import _parse_snowflake_type
        assert _parse_snowflake_type("not-json") == "VARCHAR"

    def test_missing_type_key(self):
        from ggsql_rest._snowflake import _parse_snowflake_type
        assert _parse_snowflake_type('{"nullable":true}') == "VARCHAR"


class TestStreamTableNames:
    """Test stream_table_names() generator method for streaming discovery."""

    def test_streams_per_database(self):
        """stream_table_names() yields results per database as they're discovered."""
        discovery = SnowflakeDiscovery(
            account="test-account",
            warehouse="TEST_WH",
            connection_name="my_conn",
        )
        request = _make_request({"x-user-id": "user1"})

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        call_count = {"fetchall": 0}

        def fetchall_side_effect():
            call_count["fetchall"] += 1
            if call_count["fetchall"] == 1:
                # SHOW DATABASES
                return [
                    ("created_on", "DB1", "owner", "comment", "options", "retention_time"),
                    ("created_on", "DB2", "owner", "comment", "options", "retention_time"),
                ]
            elif call_count["fetchall"] == 2:
                # SHOW TABLES IN DATABASE "DB1"
                # Row format: (created_on, name, database_name, schema_name, kind, ...)
                return [
                    ("created_on", "USERS", "DB1", "PUBLIC", "TABLE"),
                    ("created_on", "ORDERS", "DB1", "PUBLIC", "TABLE"),
                ]
            elif call_count["fetchall"] == 3:
                # SHOW TABLES IN DATABASE "DB2"
                return [
                    ("created_on", "PRODUCTS", "DB2", "PUBLIC", "TABLE"),
                ]
            raise ValueError(f"Unexpected fetchall call #{call_count['fetchall']}")

        mock_cursor.fetchall.side_effect = fetchall_side_effect

        with patch.object(discovery, "_create_connection", return_value=mock_conn):
            results = list(discovery.stream_table_names(request))

        # Should yield per-database results
        assert len(results) == 2
        db1_name, db1_tables = results[0]
        db2_name, db2_tables = results[1]

        assert db1_name == "DB1"
        assert len(db1_tables) == 2
        assert ("USERS", "DB1.PUBLIC") in db1_tables
        assert ("ORDERS", "DB1.PUBLIC") in db1_tables

        assert db2_name == "DB2"
        assert len(db2_tables) == 1
        assert ("PRODUCTS", "DB2.PUBLIC") in db2_tables

        # Should close the connection
        mock_conn.close.assert_called_once()

    def test_registers_connections_incrementally(self):
        """Connections are registered before each yield so they're queryable mid-stream."""
        discovery = SnowflakeDiscovery(
            account="test-account",
            warehouse="TEST_WH",
            connection_name="my_conn",
        )
        request = _make_request({"x-user-id": "user1"})

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        call_count = {"fetchall": 0}

        def fetchall_side_effect():
            call_count["fetchall"] += 1
            if call_count["fetchall"] == 1:
                # SHOW DATABASES
                return [
                    ("created_on", "DB1", "owner", "comment", "options", "retention_time"),
                    ("created_on", "DB2", "owner", "comment", "options", "retention_time"),
                ]
            elif call_count["fetchall"] == 2:
                # SHOW TABLES IN DATABASE "DB1"
                return [("created_on", "USERS", "DB1", "PUBLIC", "TABLE")]
            elif call_count["fetchall"] == 3:
                # SHOW TABLES IN DATABASE "DB2"
                return [("created_on", "PRODUCTS", "DB2", "PUBLIC", "TABLE")]
            raise ValueError(f"Unexpected fetchall call #{call_count['fetchall']}")

        mock_cursor.fetchall.side_effect = fetchall_side_effect

        with patch.object(discovery, "_create_connection", return_value=mock_conn):
            gen = discovery.stream_table_names(request)

            # Before any iteration, no connections registered
            assert "user1" not in discovery._discovered_connections

            # After first yield (DB1), DB1.PUBLIC should be registered
            next(gen)
            assert "user1" in discovery._discovered_connections
            assert "DB1.PUBLIC" in discovery._discovered_connections["user1"]
            assert "DB2.PUBLIC" not in discovery._discovered_connections["user1"]

            # DB1.PUBLIC is now queryable via get_engine (would work if called)
            assert discovery.has_connection("DB1.PUBLIC", request)
            assert not discovery.has_connection("DB2.PUBLIC", request)

            # After second yield (DB2), both should be registered
            next(gen)
            assert "DB2.PUBLIC" in discovery._discovered_connections["user1"]
            assert discovery.has_connection("DB2.PUBLIC", request)

    def test_populates_cache_after_full_iteration(self):
        """Consuming the full generator populates the cache."""
        discovery = SnowflakeDiscovery(
            account="test-account",
            warehouse="TEST_WH",
            connection_name="my_conn",
        )
        request = _make_request({"x-user-id": "user1"})

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        call_count = {"fetchall": 0}

        def fetchall_side_effect():
            call_count["fetchall"] += 1
            if call_count["fetchall"] == 1:
                # SHOW DATABASES
                return [("created_on", "DB1", "owner", "comment", "options", "retention_time")]
            elif call_count["fetchall"] == 2:
                # SHOW TABLES IN DATABASE "DB1"
                return [("created_on", "USERS", "DB1", "PUBLIC", "TABLE")]
            raise ValueError(f"Unexpected fetchall call #{call_count['fetchall']}")

        mock_cursor.fetchall.side_effect = fetchall_side_effect

        with patch.object(discovery, "_create_connection", return_value=mock_conn):
            # Consume the full generator
            _ = list(discovery.stream_table_names(request))

        # Should populate catalog cache
        assert "user1" in discovery._discovered_catalog
        assert discovery._discovered_catalog["user1"] == [
            ("DB1.PUBLIC", "DB1", "PUBLIC", "USERS")
        ]

        # Should populate connections cache
        assert "user1" in discovery._discovered_connections
        assert discovery._discovered_connections["user1"] == {"DB1.PUBLIC": ("DB1", "PUBLIC")}

    def test_uses_cache_on_second_call(self):
        """Second call to stream_table_names() uses cache without re-querying."""
        discovery = SnowflakeDiscovery(
            account="test-account",
            warehouse="TEST_WH",
            connection_name="my_conn",
        )
        request = _make_request({"x-user-id": "user1"})

        # Pre-populate the catalog cache with two databases
        discovery._discovered_catalog["user1"] = [
            ("DB1.PUBLIC", "DB1", "PUBLIC", "USERS"),
            ("DB1.PUBLIC", "DB1", "PUBLIC", "ORDERS"),
            ("DB2.PUBLIC", "DB2", "PUBLIC", "PRODUCTS"),
        ]
        discovery._discovered_connections["user1"] = {
            "DB1.PUBLIC": ("DB1", "PUBLIC"),
            "DB2.PUBLIC": ("DB2", "PUBLIC"),
        }

        with patch.object(discovery, "_create_connection") as mock_create:
            results = list(discovery.stream_table_names(request))

        # Should not create a new connection
        mock_create.assert_not_called()

        # Should yield cached data grouped by database
        assert len(results) == 2

        # Results should be grouped by database
        db_names = {db_name for db_name, _ in results}
        assert db_names == {"DB1", "DB2"}

        # Find DB1 and DB2 results
        db1_result = next((tables for db_name, tables in results if db_name == "DB1"), None)
        db2_result = next((tables for db_name, tables in results if db_name == "DB2"), None)

        assert db1_result is not None
        assert len(db1_result) == 2
        assert ("USERS", "DB1.PUBLIC") in db1_result
        assert ("ORDERS", "DB1.PUBLIC") in db1_result

        assert db2_result is not None
        assert len(db2_result) == 1
        assert ("PRODUCTS", "DB2.PUBLIC") in db2_result

    def test_skips_empty_databases(self):
        """Databases with no tables (after filtering INFORMATION_SCHEMA) are not yielded."""
        discovery = SnowflakeDiscovery(
            account="test-account",
            warehouse="TEST_WH",
            connection_name="my_conn",
        )
        request = _make_request({"x-user-id": "user1"})

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        call_count = {"fetchall": 0}

        def fetchall_side_effect():
            call_count["fetchall"] += 1
            if call_count["fetchall"] == 1:
                # SHOW DATABASES
                return [
                    ("created_on", "DB1", "owner", "comment", "options", "retention_time"),
                    ("created_on", "EMPTY_DB", "owner", "comment", "options", "retention_time"),
                ]
            elif call_count["fetchall"] == 2:
                # SHOW TABLES IN DATABASE "DB1"
                return [("created_on", "USERS", "DB1", "PUBLIC", "TABLE")]
            elif call_count["fetchall"] == 3:
                # SHOW TABLES IN DATABASE "EMPTY_DB" - only INFORMATION_SCHEMA tables
                return [("created_on", "COLUMNS", "EMPTY_DB", "INFORMATION_SCHEMA", "TABLE")]
            raise ValueError(f"Unexpected fetchall call #{call_count['fetchall']}")

        mock_cursor.fetchall.side_effect = fetchall_side_effect

        with patch.object(discovery, "_create_connection", return_value=mock_conn):
            results = list(discovery.stream_table_names(request))

        # Should only yield DB1, not EMPTY_DB
        assert len(results) == 1
        db_name, tables = results[0]
        assert db_name == "DB1"
        assert len(tables) == 1
        assert ("USERS", "DB1.PUBLIC") in tables

    def test_skips_inaccessible_databases(self):
        """Databases that raise exceptions during SHOW TABLES are skipped."""
        discovery = SnowflakeDiscovery(
            account="test-account",
            warehouse="TEST_WH",
            connection_name="my_conn",
        )
        request = _make_request({"x-user-id": "user1"})

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        call_count = {"execute": 0}

        def execute_side_effect(sql):
            call_count["execute"] += 1
            # Track the SQL for fetchall to respond correctly
            mock_cursor._last_sql = sql

        def fetchall_side_effect():
            sql = getattr(mock_cursor, "_last_sql", "")
            if "SHOW DATABASES" in sql:
                return [
                    ("created_on", "DB1", "owner", "comment", "options", "retention_time"),
                    ("created_on", "INACCESSIBLE_DB", "owner", "comment", "options", "retention_time"),
                ]
            elif "SHOW TABLES IN DATABASE \"DB1\"" in sql:
                return [("created_on", "USERS", "DB1", "PUBLIC", "TABLE")]
            elif "SHOW TABLES IN DATABASE \"INACCESSIBLE_DB\"" in sql:
                raise Exception("Access denied to database INACCESSIBLE_DB")
            raise ValueError(f"Unexpected SQL: {sql}")

        mock_cursor.execute.side_effect = execute_side_effect
        mock_cursor.fetchall.side_effect = fetchall_side_effect

        with patch.object(discovery, "_create_connection", return_value=mock_conn):
            results = list(discovery.stream_table_names(request))

        # Should only yield DB1, not INACCESSIBLE_DB
        assert len(results) == 1
        db_name, tables = results[0]
        assert db_name == "DB1"
        assert len(tables) == 1
        assert ("USERS", "DB1.PUBLIC") in tables
