"""Session management for isolated DuckDB instances."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from ggsql import DuckDBReader

if TYPE_CHECKING:
    import polars as pl


class Session:
    """A user session with an isolated DuckDB instance."""

    def __init__(self, session_id: str, timeout_mins: int = 30):
        self.id = session_id
        self.created_at = datetime.now(timezone.utc)
        self.last_accessed = datetime.now(timezone.utc)
        self.timeout = timedelta(minutes=timeout_mins)
        self.duckdb = DuckDBReader("duckdb://memory")
        self.tables: list[str] = []

    def touch(self) -> None:
        """Update last accessed time."""
        self.last_accessed = datetime.now(timezone.utc)

    def is_expired(self) -> bool:
        """Check if session has expired."""
        return datetime.now(timezone.utc) - self.last_accessed > self.timeout


class SessionManager:
    """Manages user sessions."""

    def __init__(
        self,
        timeout_mins: int = 30,
        seed_data: list[tuple[str, pl.DataFrame]] | None = None,
    ):
        self._sessions: dict[str, Session] = {}
        self._timeout_mins = timeout_mins
        self._seed_data = seed_data or []

    def create(self) -> Session:
        """Create a new session, seeded with base tables if configured."""
        self.cleanup_expired()
        session_id = uuid.uuid4().hex
        session = Session(session_id, self._timeout_mins)
        for table_name, df in self._seed_data:
            session.duckdb.register(table_name, df)
            session.tables.append(table_name)
        self._sessions[session_id] = session
        return session

    def get(self, session_id: str) -> Session | None:
        """Get a session by ID, or None if not found or expired."""
        session = self._sessions.get(session_id)
        if session is None:
            return None
        if session.is_expired():
            del self._sessions[session_id]
            return None
        session.touch()
        return session

    def delete(self, session_id: str) -> bool:
        """Delete a session. Returns True if deleted, False if not found."""
        return self._sessions.pop(session_id, None) is not None

    def cleanup_expired(self) -> None:
        """Remove all expired sessions."""
        expired = [sid for sid, s in self._sessions.items() if s.is_expired()]
        for sid in expired:
            del self._sessions[sid]


def load_seed_data(paths: list[str]) -> list[tuple[str, pl.DataFrame]]:
    """Load data files into (table_name, DataFrame) pairs for session seeding.

    Supports CSV, Parquet, JSON, JSONL, and NDJSON files.
    Table names are derived from filenames (without extension).
    """
    import re
    from pathlib import Path

    import polars as pl  # noqa: PLW0621

    seed: list[tuple[str, pl.DataFrame]] = []
    for path_str in paths:
        p = Path(path_str)
        if not p.exists():
            raise FileNotFoundError(f"Data file not found: {path_str}")

        ext = p.suffix.lower()
        if ext == ".csv":
            df = pl.read_csv(p)
        elif ext == ".parquet":
            df = pl.read_parquet(p)
        elif ext in (".json", ".jsonl", ".ndjson"):
            df = pl.read_json(p)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

        # Derive table name from filename
        name = re.sub(r"[^a-zA-Z0-9_]", "_", p.stem)
        name = re.sub(r"_+", "_", name).strip("_") or "unnamed"

        seed.append((name, df))
    return seed
