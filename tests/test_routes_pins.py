"""Tests for pins integration in schema routes."""

import json
import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from httpx import ASGITransport, AsyncClient

from ggsql_rest import create_app, ConnectionRegistry
from ggsql_rest._pins import PinEntry, PinsDiscovery

_TEST_API_KEY = "test-api-key"
_TEST_ENV = {
    "CONNECT_API_KEY": _TEST_API_KEY,
    "CONNECT_SERVER": "https://connect.example.com",
}


@pytest.fixture
def pins_discovery():
    """Create a PinsDiscovery with pre-populated cache."""
    discovery = PinsDiscovery()
    discovery._discovered_pins[_TEST_API_KEY] = [
        PinEntry("alice__sales", "alice/sales"),
        PinEntry("alice__revenue", "alice/revenue"),
        PinEntry("bob__data", "bob/data"),
    ]
    return discovery


@pytest.fixture
def app_with_pins(pins_discovery):
    """Create a test app with pins discovery enabled."""
    registry = ConnectionRegistry()
    return create_app(registry, pins=pins_discovery)


@pytest.mark.anyio
async def test_streaming_schema_includes_pins(app_with_pins):
    """Streaming /schema/tables should include pins grouped by owner."""
    with patch.dict(os.environ, _TEST_ENV):
        async with AsyncClient(
            transport=ASGITransport(app=app_with_pins), base_url="http://test"
        ) as client:
            resp = await client.post("/api/v1/sessions")
            session_id = resp.json()["data"]["sessionId"]

            resp = await client.get(
                f"/api/v1/sessions/{session_id}/schema/tables?stream=true"
            )
            assert resp.status_code == 200

            lines = [
                json.loads(line)
                for line in resp.text.strip().split("\n")
                if line.strip()
            ]

            all_tables = []
            for line in lines:
                all_tables.extend(line["tables"])

            pin_tables = [t for t in all_tables if t.get("provider") == "pins"]
            assert len(pin_tables) == 3
            assert any(t["tableName"] == "alice__sales" for t in pin_tables)
            assert any(t["tableName"] == "alice__revenue" for t in pin_tables)
            assert any(t["tableName"] == "bob__data" for t in pin_tables)

            # Connection should be the owner name, not "pins"
            alice_tables = [t for t in pin_tables if t["source"] == "alice"]
            bob_tables = [t for t in pin_tables if t["source"] == "bob"]
            assert len(alice_tables) == 2
            assert len(bob_tables) == 1


@pytest.mark.anyio
async def test_non_streaming_schema_includes_pins(app_with_pins):
    """/schema/tables (now always NDJSON) should include pins."""
    with patch.dict(os.environ, _TEST_ENV):
        async with AsyncClient(
            transport=ASGITransport(app=app_with_pins), base_url="http://test"
        ) as client:
            resp = await client.post("/api/v1/sessions")
            session_id = resp.json()["data"]["sessionId"]

            resp = await client.get(f"/api/v1/sessions/{session_id}/schema/tables")
            assert resp.status_code == 200
            assert resp.headers["content-type"] == "application/x-ndjson"

            # Parse NDJSON lines and collect all tables
            lines = resp.text.strip().split("\n")
            all_tables = []
            for line in lines:
                batch = json.loads(line)
                all_tables.extend(batch["tables"])

            pin_tables = [t for t in all_tables if t.get("provider") == "pins"]
            assert len(pin_tables) == 3


@pytest.mark.anyio
async def test_query_with_pins_connection_loads_data():
    """Query with connection='pins' should trigger pin loading before execution."""
    test_df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    mock_board = MagicMock()
    mock_board.pin_read.return_value = test_df

    discovery = PinsDiscovery()
    discovery._discovered_pins[_TEST_API_KEY] = [
        PinEntry("test__data", "test/data"),
    ]

    app = create_app(ConnectionRegistry(), pins=discovery)

    with (
        patch("pins.board_connect", return_value=mock_board),
        patch.dict(os.environ, _TEST_ENV),
    ):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.post("/api/v1/sessions")
            session_id = resp.json()["data"]["sessionId"]

            resp = await client.post(
                f"/api/v1/sessions/{session_id}/sql",
                json={"query": "SELECT * FROM test__data", "source": "pins"},
            )
            assert resp.status_code == 200
            data = resp.json()["data"]
            assert data["rowCount"] == 3
