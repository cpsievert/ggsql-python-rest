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
