import os
from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl
import pytest
from starlette.requests import Request

from ggsql_rest._pins import PinEntry, PinsDiscovery, sanitize_pin_name

# All tests use CONNECT_API_KEY fallback (local dev mode)
_TEST_API_KEY = "test-api-key"
_TEST_SERVER = "https://connect.example.com"
_TEST_ENV = {"CONNECT_API_KEY": _TEST_API_KEY, "CONNECT_SERVER": _TEST_SERVER}


def _make_request(headers: dict[str, str] | None = None) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [
            (k.lower().encode(), v.encode()) for k, v in (headers or {}).items()
        ],
    }
    return Request(scope)


def _mock_connect_client(
    pin_items: list[dict],
    users: list[dict] | None = None,
) -> MagicMock:
    """Auto-generates users from pin_items if not provided."""
    if users is None:
        seen: dict[str, str] = {}
        for item in pin_items:
            guid = item["owner_guid"]
            if guid not in seen:
                seen[guid] = guid.replace("guid-", "")  # e.g. "guid-alice" -> "alice"
        users = [{"guid": g, "username": u} for g, u in seen.items()]

    mock_client = MagicMock()
    mock_client.users.find.return_value = users
    mock_client.content.find.return_value = pin_items
    return mock_client


def _pin_item(name: str, owner: str) -> dict:
    return {"name": name, "owner_guid": f"guid-{owner}", "content_category": "pin"}


_TEST_PANDAS_DF = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})


class TestSanitizePinName:
    def test_simple_pin_name(self):
        assert sanitize_pin_name("alice/sales_data") == "alice__sales_data"

    def test_no_slash(self):
        assert sanitize_pin_name("sales_data") == "sales_data"

    def test_multiple_slashes(self):
        assert sanitize_pin_name("org/team/dataset") == "org__team__dataset"

    def test_special_characters(self):
        assert sanitize_pin_name("alice/my-data.v2") == "alice__my_data_v2"

    def test_leading_trailing_underscores(self):
        assert sanitize_pin_name("/leading/") == "leading"

    def test_consecutive_underscores_collapsed(self):
        assert sanitize_pin_name("alice//double") == "alice__double"


class TestPinsDiscoveryStreamTableNames:
    def test_discovers_all_pins_grouped_by_owner(self):
        mock_client = _mock_connect_client(
            [
                _pin_item("sales", "alice"),
                _pin_item("model", "alice"),
                _pin_item("revenue", "bob"),
                _pin_item("config", "bob"),
            ]
        )

        with (
            patch("posit.connect.Client", return_value=mock_client),
            patch.dict(os.environ, _TEST_ENV),
        ):
            discovery = PinsDiscovery()
            request = _make_request()
            batches = list(discovery.stream_table_names(request))

        # Two owners -> two batches
        assert len(batches) == 2
        batch_dict = {owner: entries for owner, entries in batches}

        assert "alice" in batch_dict
        assert "alice__sales" in batch_dict["alice"]
        assert "alice__model" in batch_dict["alice"]

        assert "bob" in batch_dict
        assert "bob__revenue" in batch_dict["bob"]
        assert "bob__config" in batch_dict["bob"]

    def test_caches_by_api_key(self):
        """Client constructed once, not per call."""
        mock_client = _mock_connect_client([_pin_item("data", "alice")])

        with (
            patch("posit.connect.Client", return_value=mock_client) as mock_cls,
            patch.dict(os.environ, _TEST_ENV),
        ):
            discovery = PinsDiscovery()
            request = _make_request()

            list(discovery.stream_table_names(request))
            list(discovery.stream_table_names(request))

            assert mock_cls.call_count == 1

    def test_different_api_keys_discover_separately(self):
        mock_client = _mock_connect_client([_pin_item("data", "alice")])

        with (
            patch("posit.connect.Client", return_value=mock_client) as mock_cls,
            patch.dict(os.environ, _TEST_ENV),
        ):
            discovery = PinsDiscovery()

            # Pre-populate token cache to avoid mocking posit-sdk
            discovery._token_cache["token-1"] = "key-1"
            discovery._token_cache["token-2"] = "key-2"

            req1 = _make_request({"Posit-Connect-User-Session-Token": "token-1"})
            req2 = _make_request({"Posit-Connect-User-Session-Token": "token-2"})

            list(discovery.stream_table_names(req1))
            list(discovery.stream_table_names(req2))

            assert mock_cls.call_count == 2

    def test_empty_pin_list(self):
        mock_client = _mock_connect_client([], users=[])

        with (
            patch("posit.connect.Client", return_value=mock_client),
            patch.dict(os.environ, _TEST_ENV),
        ):
            discovery = PinsDiscovery()
            request = _make_request()
            batches = list(discovery.stream_table_names(request))

        assert len(batches) == 0

    def test_filters_non_pin_content(self):
        mock_client = _mock_connect_client(
            [
                _pin_item("sales", "alice"),
                {
                    "name": "my_shiny_app",
                    "owner_guid": "guid-alice",
                    "content_category": "shiny",
                },
                _pin_item("model", "alice"),
                {
                    "name": "dashboard",
                    "owner_guid": "guid-bob",
                    "content_category": "quarto",
                },
                _pin_item("revenue", "bob"),
            ]
        )

        with (
            patch("posit.connect.Client", return_value=mock_client),
            patch.dict(os.environ, _TEST_ENV),
        ):
            discovery = PinsDiscovery()
            request = _make_request()
            batches = list(discovery.stream_table_names(request))

        # Two owners -> two batches
        assert len(batches) == 2
        batch_dict = {owner: entries for owner, entries in batches}

        # Alice: two pins, not the shiny app
        assert "alice" in batch_dict
        assert len(batch_dict["alice"]) == 2
        assert "alice__sales" in batch_dict["alice"]
        assert "alice__model" in batch_dict["alice"]
        assert "alice__my_shiny_app" not in batch_dict["alice"]

        # Bob: one pin, not the quarto dashboard
        assert "bob" in batch_dict
        assert len(batch_dict["bob"]) == 1
        assert "bob__revenue" in batch_dict["bob"]
        assert "bob__dashboard" not in batch_dict["bob"]

    def test_session_token_exchange(self):
        """Exchanges session token for viewer API key on Connect."""
        mock_discovery_client = _mock_connect_client([_pin_item("data", "alice")])

        mock_viewer_client = MagicMock()
        mock_viewer_client.cfg.api_key = "viewer-api-key-123"

        mock_owner_client = MagicMock()
        mock_owner_client.with_user_session_token.return_value = mock_viewer_client

        call_count = 0

        def client_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call: _resolve_api_key (no api_key kwarg)
                return mock_owner_client
            else:
                # Second call: _discover_and_stream (with api_key kwarg)
                return mock_discovery_client

        with (
            patch("posit.connect.Client", side_effect=client_side_effect),
            patch.dict(os.environ, _TEST_ENV),
        ):
            discovery = PinsDiscovery()
            request = _make_request(
                {"Posit-Connect-User-Session-Token": "session-token-abc"}
            )
            batches = list(discovery.stream_table_names(request))

        mock_owner_client.with_user_session_token.assert_called_once_with(
            "session-token-abc"
        )
        assert len(batches) == 1

    def test_session_token_cached(self):
        """Caches exchanged API key."""
        mock_discovery_client = _mock_connect_client([_pin_item("data", "alice")])

        mock_viewer_client = MagicMock()
        mock_viewer_client.cfg.api_key = "viewer-api-key-123"

        mock_owner_client = MagicMock()
        mock_owner_client.with_user_session_token.return_value = mock_viewer_client

        call_count = 0

        def client_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return mock_owner_client
            else:
                return mock_discovery_client

        with (
            patch("posit.connect.Client", side_effect=client_side_effect),
            patch.dict(os.environ, _TEST_ENV),
        ):
            discovery = PinsDiscovery()
            request = _make_request(
                {"Posit-Connect-User-Session-Token": "session-token-abc"}
            )

            list(discovery.stream_table_names(request))
            list(discovery.stream_table_names(request))

            mock_owner_client.with_user_session_token.assert_called_once()

    def test_no_auth_yields_nothing_with_warning(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("CONNECT_API_KEY", None)

            discovery = PinsDiscovery()
            request = _make_request()  # No session token header

            with pytest.warns(UserWarning, match="Pins authentication requires"):
                batches = list(discovery.stream_table_names(request))
            assert len(batches) == 0


class TestEnsurePinLoaded:
    def test_loads_pin_via_pin_read(self):
        """Uses board.pin_read() and registers in DuckDB."""
        mock_board = MagicMock()
        mock_board.pin_read.return_value = _TEST_PANDAS_DF

        with (
            patch("pins.board_connect", return_value=mock_board),
            patch.dict(os.environ, _TEST_ENV),
        ):
            discovery = PinsDiscovery()
            discovery._discovered_pins[_TEST_API_KEY] = [
                PinEntry("alice__data", "alice/data")
            ]
            request = _make_request()

            mock_session = MagicMock()
            mock_session.tables = []
            discovery.ensure_pin_loaded(
                "session-1", "alice__data", request, mock_session
            )

        mock_board.pin_read.assert_called_once_with("alice/data")
        mock_session.duckdb.register.assert_called_once()
        call_args = mock_session.duckdb.register.call_args
        assert call_args[0][0] == "alice__data"
        assert isinstance(call_args[0][1], pl.DataFrame)
        assert call_args[0][1].shape == (3, 2)
        assert "alice__data" in mock_session.tables

    def test_no_op_if_already_loaded(self):
        """Skips re-download if already loaded."""
        mock_board = MagicMock()

        with (
            patch("pins.board_connect", return_value=mock_board),
            patch.dict(os.environ, _TEST_ENV),
        ):
            discovery = PinsDiscovery()
            discovery._discovered_pins[_TEST_API_KEY] = [
                PinEntry("alice__data", "alice/data")
            ]
            discovery._loaded_pins["session-1"] = {"alice__data"}
            request = _make_request()

            mock_session = MagicMock()
            mock_session.tables = []
            discovery.ensure_pin_loaded(
                "session-1", "alice__data", request, mock_session
            )

        mock_board.pin_read.assert_not_called()
        mock_session.duckdb.register.assert_not_called()

    def test_unknown_pin_raises(self):
        """Raises KeyError for unknown pins."""
        with (
            patch("pins.board_connect"),
            patch.dict(os.environ, _TEST_ENV),
        ):
            discovery = PinsDiscovery()
            discovery._discovered_pins[_TEST_API_KEY] = []
            request = _make_request()

            mock_session = MagicMock()
            with pytest.raises(KeyError, match="not found"):
                discovery.ensure_pin_loaded(
                    "session-1", "nonexistent", request, mock_session
                )


class TestGetPinSchema:
    def test_parquet_schema_only(self, tmp_path):
        """Reads schema without loading full data."""
        parquet_path = tmp_path / "data.parquet"
        pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}).write_parquet(
            parquet_path
        )

        mock_board = MagicMock()
        mock_board.pin_download.return_value = [str(parquet_path)]

        with (
            patch("pins.board_connect", return_value=mock_board),
            patch.dict(os.environ, _TEST_ENV),
        ):
            discovery = PinsDiscovery()
            discovery._discovered_pins[_TEST_API_KEY] = [
                PinEntry("alice__data", "alice/data")
            ]
            request = _make_request()

            columns = discovery.get_pin_schema("alice__data", request)

        mock_board.pin_download.assert_called_once_with("alice/data")
        mock_board.pin_read.assert_not_called()
        assert len(columns) == 2
        assert columns[0][0] == "id"
        assert columns[1][0] == "name"

    def test_csv_schema_only(self, tmp_path):
        """Reads CSV header without loading full data."""
        csv_path = tmp_path / "data.csv"
        pl.DataFrame({"x": [1], "y": [2.0], "z": ["a"]}).write_csv(csv_path)

        mock_board = MagicMock()
        mock_board.pin_download.return_value = [str(csv_path)]

        with (
            patch("pins.board_connect", return_value=mock_board),
            patch.dict(os.environ, _TEST_ENV),
        ):
            discovery = PinsDiscovery()
            discovery._discovered_pins[_TEST_API_KEY] = [
                PinEntry("alice__data", "alice/data")
            ]
            request = _make_request()

            columns = discovery.get_pin_schema("alice__data", request)

        assert len(columns) == 3
        assert columns[0][0] == "x"
        assert columns[1][0] == "y"
        assert columns[2][0] == "z"

    def test_arrow_schema_only(self, tmp_path):
        """Reads Arrow/Feather schema without loading full data."""
        import pyarrow as pa
        import pyarrow.feather as pf

        arrow_path = tmp_path / "data.arrow"
        table = pa.table({"a": [1, 2], "b": ["x", "y"]})
        pf.write_feather(table, arrow_path)

        mock_board = MagicMock()
        mock_board.pin_download.return_value = [str(arrow_path)]

        with (
            patch("pins.board_connect", return_value=mock_board),
            patch.dict(os.environ, _TEST_ENV),
        ):
            discovery = PinsDiscovery()
            discovery._discovered_pins[_TEST_API_KEY] = [
                PinEntry("alice__data", "alice/data")
            ]
            request = _make_request()

            columns = discovery.get_pin_schema("alice__data", request)

        assert len(columns) == 2
        assert columns[0][0] == "a"
        assert columns[1][0] == "b"
