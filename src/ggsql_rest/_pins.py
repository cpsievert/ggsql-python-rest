"""Posit Connect Pins discovery and on-demand loading."""

from __future__ import annotations

import os
import re
import warnings
from typing import TYPE_CHECKING, Iterator, NamedTuple

import polars as pl
from starlette.requests import Request

if TYPE_CHECKING:
    from ggsql_rest._sessions import Session

from ggsql_rest._constants import SESSION_TOKEN_HEADER


class PinEntry(NamedTuple):
    sanitized: str  # DuckDB table name, e.g. "carson__test_sales"
    original: str  # owner/name format, e.g. "carson/test_sales"


class PinsDiscovery:
    """Discovers and loads Posit Connect pins as DuckDB tables.

    Auth: session token exchange on Connect, CONNECT_API_KEY locally.
    """

    def __init__(self) -> None:
        self._discovered_pins: dict[str, list[PinEntry]] = {}
        self._loaded_pins: dict[str, set[str]] = {}
        self._token_cache: dict[str, str] = {}  # session_token -> viewer api_key

    def stream_table_names(
        self, request: Request
    ) -> Iterator[tuple[str, list[str]]]:
        """Yield ``(owner, [table_name, ...])`` batches. Discovery is cached per viewer."""
        try:
            api_key = self._resolve_api_key(request)
        except ValueError as exc:
            warnings.warn(str(exc), stacklevel=2)
            return

        if api_key in self._discovered_pins:
            yield from group_pins_by_owner(self._discovered_pins[api_key])
            return

        yield from self._discover_and_stream(api_key)

    def _discover_and_stream(
        self, api_key: str
    ) -> Iterator[tuple[str, list[str]]]:
        """Discover pins via posit-sdk content API.

        content.find(content_type='pin') doesn't actually filter — use content_category instead.
        """
        from posit.connect import Client

        client = Client(api_key=api_key)

        users = client.users.find()
        user_map: dict[str, str] = {u["guid"]: u["username"] for u in users}

        all_content = client.content.find()
        pin_content = [
            p for p in all_content if p.get("content_category") == "pin"
        ]

        by_owner: dict[str, list[dict]] = {}  # type: ignore[type-arg]
        for p in pin_content:
            by_owner.setdefault(p["owner_guid"], []).append(p)

        self._discovered_pins[api_key] = []

        for owner_guid, items in by_owner.items():
            owner = user_map.get(owner_guid, "unknown")
            table_names: list[str] = []
            for p in items:
                original = f"{owner}/{p['name']}"
                sanitized = sanitize_pin_name(original)
                self._discovered_pins[api_key].append(PinEntry(sanitized, original))
                table_names.append(sanitized)

            yield owner, table_names

    def _ensure_discovered(self, api_key: str) -> None:
        if api_key not in self._discovered_pins:
            for _ in self._discover_and_stream(api_key):
                pass

    def load_pins_for_query(
        self,
        query: str,
        request: Request,
        session: Session,
    ) -> None:
        """Load any pins referenced in the query into the session's DuckDB."""
        api_key = self._resolve_api_key(request)
        self._ensure_discovered(api_key)

        for pin in self._discovered_pins[api_key]:
            if pin.sanitized in query:
                self.ensure_pin_loaded(session.id, pin.sanitized, request, session)

    def ensure_pin_loaded(
        self,
        session_id: str,
        table_name: str,
        request: Request,
        session: Session,
    ) -> None:
        """Load a pin into DuckDB if not already present."""
        if session_id in self._loaded_pins and table_name in self._loaded_pins[session_id]:
            return

        pin = self._find_pin(table_name, request)
        if pin is None:
            raise KeyError(f"Pin '{table_name}' not found in discovered pins")

        api_key = self._resolve_api_key(request)
        board = make_board(api_key)
        pandas_df = board.pin_read(pin.original)
        df = pl.from_pandas(pandas_df)

        session.duckdb.register(table_name, df)
        session.tables.append(table_name)

        if session_id not in self._loaded_pins:
            self._loaded_pins[session_id] = set()
        self._loaded_pins[session_id].add(table_name)

    def get_pin_schema(
        self,
        table_name: str,
        request: Request,
    ) -> list[tuple[str, str]]:
        """Get column schema without loading full data."""
        pin = self._find_pin(table_name, request)
        if pin is None:
            return []

        api_key = self._resolve_api_key(request)
        board = make_board(api_key)
        try:
            file_paths = board.pin_download(pin.original)
        except Exception as exc:
            warnings.warn(
                f"Failed to download pin '{pin.original}': {exc}",
                stacklevel=2,
            )
            return []
        if not file_paths:
            return []

        return read_file_schema(file_paths[0])

    def has_pin(self, table_name: str, request: Request) -> bool:
        return self._find_pin(table_name, request) is not None

    def has_any_pin_for_query(self, query: str, request: Request) -> bool:
        return any(pin.sanitized in query for pin in self._get_pins(request))

    def _get_pins(self, request: Request) -> list[PinEntry]:
        """Get discovered pins for this viewer, or empty list if unavailable."""
        try:
            api_key = self._resolve_api_key(request)
        except ValueError:
            return []
        return self._discovered_pins.get(api_key, [])

    def _find_pin(self, table_name: str, request: Request) -> PinEntry | None:
        return next(
            (pin for pin in self._get_pins(request) if pin.sanitized == table_name),
            None,
        )

    def _resolve_api_key(self, request: Request) -> str:
        """Exchange session token for viewer API key, or use CONNECT_API_KEY env var."""
        session_token = request.headers.get(SESSION_TOKEN_HEADER)

        if session_token:
            if session_token in self._token_cache:
                return self._token_cache[session_token]

            from posit.connect import Client

            owner_client = Client()
            viewer_client = owner_client.with_user_session_token(session_token)
            viewer_api_key = viewer_client.cfg.api_key
            self._token_cache[session_token] = viewer_api_key
            if viewer_api_key is None:
                raise ValueError("Failed to resolve API key from session token")
            return viewer_api_key

        api_key = os.environ.get("CONNECT_API_KEY", "")
        if not api_key:
            raise ValueError(
                "Pins authentication requires either a "
                "Posit-Connect-User-Session-Token header (on Connect) "
                "or CONNECT_API_KEY env var (local dev)."
            )
        return api_key


# --- Pure helper functions ---


def make_board(api_key: str):
    """board_connect(api_key=...) doesn't auto-detect CONNECT_SERVER, so pass both explicitly."""
    from pins import board_connect

    server_url = os.environ.get("CONNECT_SERVER", "")
    return board_connect(server_url=server_url, api_key=api_key)


def sanitize_pin_name(pin_name: str) -> str:
    """Convert pin name to a valid DuckDB table name."""
    name = pin_name.replace("/", "__")
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    name = re.sub(r"_{3,}", "__", name)
    name = name.strip("_")
    return name or "unnamed_pin"


def read_file_schema(path: str) -> list[tuple[str, str]]:
    """Read column names and types from a tabular file without loading all data.

    Parquet/Arrow: reads metadata only. CSV/TSV: header only.
    Returns empty list for unknown formats.
    """
    lower = path.lower()

    if lower.endswith(".parquet"):
        import pyarrow.parquet as pq

        schema = pq.read_schema(path)
        return [(f.name, str(f.type)) for f in schema]

    if lower.endswith((".arrow", ".feather")):
        import pyarrow.ipc as ipc

        with ipc.open_file(path) as reader:
            schema = reader.schema
        return [(f.name, str(f.type)) for f in schema]

    if lower.endswith(".csv"):
        df = pl.read_csv(path, n_rows=0)
        return [(name, str(dtype)) for name, dtype in df.schema.items()]

    if lower.endswith(".tsv"):
        df = pl.read_csv(path, n_rows=0)
        return [(name, str(dtype)) for name, dtype in df.schema.items()]

    return []


def group_pins_by_owner(
    pins: list[PinEntry],
) -> Iterator[tuple[str, list[str]]]:
    """Group pin entries by owner, yielding ``(owner, [sanitized_name, ...])``."""
    owners: dict[str, list[str]] = {}
    for pin in pins:
        owner = pin.original.split("/")[0] if "/" in pin.original else "unknown"
        owners.setdefault(owner, []).append(pin.sanitized)

    yield from owners.items()
