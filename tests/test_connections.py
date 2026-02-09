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
