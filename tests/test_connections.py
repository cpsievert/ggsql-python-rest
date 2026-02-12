"""Tests for connection registry."""

import pytest
from unittest.mock import MagicMock
from sqlalchemy import create_engine

from ggsql_rest._connections import ConnectionRegistry


def test_register_and_list():
    registry = ConnectionRegistry()
    registry.register("test", lambda req: create_engine("sqlite:///:memory:"))
    assert "test" in registry.list_connections()


def test_get_engine():
    registry = ConnectionRegistry()
    engine = create_engine("sqlite:///:memory:")
    registry.register("test", lambda req: engine)

    mock_request = MagicMock()
    mock_request.headers = {}

    result = registry.get_engine("test", mock_request)
    assert result is engine


def test_get_engine_caches_by_user():
    registry = ConnectionRegistry()
    call_count = 0

    def factory(req):
        nonlocal call_count
        call_count += 1
        return create_engine("sqlite:///:memory:")

    registry.register("test", factory)

    mock_request = MagicMock()
    mock_request.headers = {"X-User-Id": "user1"}

    # First call creates engine
    registry.get_engine("test", mock_request)
    assert call_count == 1

    # Second call with same user returns cached
    registry.get_engine("test", mock_request)
    assert call_count == 1

    # Different user creates new engine
    mock_request.headers = {"X-User-Id": "user2"}
    registry.get_engine("test", mock_request)
    assert call_count == 2


def test_get_engine_unknown():
    registry = ConnectionRegistry()
    mock_request = MagicMock()
    mock_request.headers = {}

    with pytest.raises(KeyError, match="Unknown connection"):
        registry.get_engine("nonexistent", mock_request)


def test_extract_user_id():
    registry = ConnectionRegistry()

    mock_request = MagicMock()
    mock_request.headers = {"X-User-Id": "user123"}
    assert registry._extract_user_id(mock_request) == "user123"

    mock_request.headers = {}
    assert registry._extract_user_id(mock_request) == "anonymous"


def test_engine_cache_evicts_lru():
    """Engines beyond max_engines are evicted (least-recently-used first)."""
    registry = ConnectionRegistry(max_engines=2)

    engines = {}

    def factory(req):
        user = req.headers.get("X-User-Id", "anon")
        e = create_engine("sqlite:///:memory:")
        engines[user] = e
        return e

    registry.register("db", factory)

    def req(user: str):
        mock = MagicMock()
        mock.headers = {"X-User-Id": user}
        return mock

    # Fill cache to capacity
    registry.get_engine("db", req("u1"))
    registry.get_engine("db", req("u2"))
    assert len(registry._engines) == 2

    # Adding a third evicts u1 (the LRU)
    registry.get_engine("db", req("u3"))
    assert len(registry._engines) == 2
    assert ("db", "u1") not in registry._engines
    assert ("db", "u3") in registry._engines


def test_get_provider_returns_registered_provider():
    """get_provider returns the provider string when registered."""
    registry = ConnectionRegistry()
    registry.register("my_pg", lambda req: create_engine("sqlite:///:memory:"), provider="postgresql")
    assert registry.get_provider("my_pg") == "postgresql"


def test_get_provider_returns_none_for_unknown():
    """get_provider returns None for unknown connection names."""
    registry = ConnectionRegistry()
    assert registry.get_provider("nonexistent") is None


def test_get_provider_returns_none_when_no_provider_set():
    """get_provider returns None when connection has no provider."""
    registry = ConnectionRegistry()
    registry.register("legacy", lambda req: create_engine("sqlite:///:memory:"))
    assert registry.get_provider("legacy") is None


def test_register_with_multiple_providers():
    """Multiple connections can have different providers."""
    registry = ConnectionRegistry()
    registry.register("pg_conn", lambda req: create_engine("sqlite:///:memory:"), provider="postgresql")
    registry.register("mysql_conn", lambda req: create_engine("sqlite:///:memory:"), provider="mysql")
    registry.register("no_provider", lambda req: create_engine("sqlite:///:memory:"))

    assert registry.get_provider("pg_conn") == "postgresql"
    assert registry.get_provider("mysql_conn") == "mysql"
    assert registry.get_provider("no_provider") is None
