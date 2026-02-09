# Cleanup and Polish Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Address remaining review items 6-13: UTC datetimes, upload table name safety, expired session cleanup, version dedup, dependency consolidation, missing test dep, dead sentinel code, and full table name sanitization.

**Architecture:** Small, surgical changes across several modules. No new modules. Most tasks are independent.

**Tech Stack:** Python datetime, re, importlib.metadata, pytest, pyproject.toml

---

### Task 1: Use UTC-aware datetimes in sessions

**Files:**
- Modify: `src/ggsql_rest/_sessions.py`
- Test: `tests/test_sessions.py`

**Step 1: Write the failing test**

Add to `tests/test_sessions.py`:

```python
from datetime import timezone

def test_session_uses_utc():
    session = Session("test", timeout_mins=30)
    assert session.created_at.tzinfo == timezone.utc
    assert session.last_accessed.tzinfo == timezone.utc
    session.touch()
    assert session.last_accessed.tzinfo == timezone.utc
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_sessions.py::test_session_uses_utc -v`
Expected: FAIL — `session.created_at.tzinfo` is `None`

**Step 3: Implement**

In `src/ggsql_rest/_sessions.py`, change `from datetime import datetime, timedelta` to `from datetime import datetime, timedelta, timezone`.

Replace all 3 occurrences of `datetime.now()` with `datetime.now(timezone.utc)`:
- Line 14: `self.created_at = datetime.now(timezone.utc)`
- Line 15: `self.last_accessed = datetime.now(timezone.utc)`
- Line 22: `self.last_accessed = datetime.now(timezone.utc)`
- Line 26: `return datetime.now(timezone.utc) - self.last_accessed > self.timeout`

**Step 4: Run all session tests**

Run: `uv run pytest tests/test_sessions.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/ggsql_rest/_sessions.py tests/test_sessions.py
git commit -m "fix: use UTC-aware datetimes in session management"
```

---

### Task 2: Full table name sanitization on upload

**Files:**
- Modify: `src/ggsql_rest/_routes/_sessions.py`
- Test: `tests/test_routes_upload.py`

This replaces the simple `.replace("-", "_").replace(" ", "_")` with full sanitization: strip non-alphanumeric/underscore chars, prefix leading digits, and add `_upload_` prefix to namespace away from `__remote_result_*` internal tables.

**Step 1: Write failing tests**

Add to `tests/test_routes_upload.py`:

```python
@pytest.mark.anyio
async def test_upload_sanitizes_special_chars():
    app, session_mgr = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        csv_content = b"x,y\n1,2"
        files = {"file": ("my@data!file.csv", io.BytesIO(csv_content), "text/csv")}

        response = await client.post(f"/sessions/{session.id}/upload", files=files)

        assert response.status_code == 200
        # Special chars stripped, prefixed with _upload_
        assert response.json()["table_name"] == "_upload_my_data_file"


@pytest.mark.anyio
async def test_upload_sanitizes_leading_digit():
    app, session_mgr = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        csv_content = b"x,y\n1,2"
        files = {"file": ("2024-data.csv", io.BytesIO(csv_content), "text/csv")}

        response = await client.post(f"/sessions/{session.id}/upload", files=files)

        assert response.status_code == 200
        # Leading digit gets _ prefix via _upload_ prefix
        assert response.json()["table_name"] == "_upload_2024_data"


@pytest.mark.anyio
async def test_upload_deduplicates_table_name():
    app, session_mgr = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        session = session_mgr.create()

        csv_content = b"x,y\n1,2"

        # Upload same filename twice
        files = {"file": ("data.csv", io.BytesIO(csv_content), "text/csv")}
        resp1 = await client.post(f"/sessions/{session.id}/upload", files=files)
        assert resp1.status_code == 200
        assert resp1.json()["table_name"] == "_upload_data"

        files = {"file": ("data.csv", io.BytesIO(csv_content), "text/csv")}
        resp2 = await client.post(f"/sessions/{session.id}/upload", files=files)
        assert resp2.status_code == 200
        assert resp2.json()["table_name"] == "_upload_data_2"

        # Both tables should be tracked
        assert len(session.tables) == 2
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_routes_upload.py::test_upload_sanitizes_special_chars tests/test_routes_upload.py::test_upload_sanitizes_leading_digit tests/test_routes_upload.py::test_upload_deduplicates_table_name -v`
Expected: FAIL

**Step 3: Implement sanitization**

In `src/ggsql_rest/_routes/_sessions.py`, add `import re` at the top.

Replace the table name derivation logic in `upload_file`. Replace lines 72-73:

```python
    # Derive table name from filename
    table_name = Path(file.filename).stem.replace("-", "_").replace(" ", "_")
```

with:

```python
    # Derive safe table name from filename
    table_name = _sanitize_table_name(Path(file.filename).stem, session.tables)
```

Add a module-level helper function (above `upload_file`, after the router definition area):

```python
def _sanitize_table_name(stem: str, existing_tables: list[str]) -> str:
    """Sanitize a filename stem into a safe, unique DuckDB table name."""
    # Replace non-alphanumeric chars with underscore
    name = re.sub(r"[^a-zA-Z0-9_]", "_", stem)
    # Collapse multiple underscores
    name = re.sub(r"_+", "_", name)
    # Strip leading/trailing underscores
    name = name.strip("_")
    # Prefix to namespace away from __remote_result_* internal tables
    name = f"_upload_{name}" if name else "_upload_unnamed"

    # Deduplicate if name already exists
    base_name = name
    counter = 2
    while name in existing_tables:
        name = f"{base_name}_{counter}"
        counter += 1

    return name
```

**Step 4: Update existing test expectations**

The existing tests expect unprefixed names. Update:
- `test_upload_csv`: expect `"_upload_data"` instead of `"data"`
- `test_upload_parquet`: expect `"_upload_data"` instead of `"data"`
- `test_upload_json`: expect `"_upload_data"` instead of `"data"`
- `test_upload_filename_sanitization`: expect `"_upload_my_data_file"` instead of `"my_data_file"`
- Also update `test_upload_csv`'s assertion `assert "_upload_data" in session.tables`

**Step 5: Run all upload tests**

Run: `uv run pytest tests/test_routes_upload.py -v`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/ggsql_rest/_routes/_sessions.py tests/test_routes_upload.py
git commit -m "fix: full table name sanitization with _upload_ prefix and dedup"
```

---

### Task 3: Call `cleanup_expired` on every session create

**Files:**
- Modify: `src/ggsql_rest/_sessions.py`
- Test: `tests/test_sessions.py`

**Step 1: Write the failing test**

Add to `tests/test_sessions.py`:

```python
def test_create_triggers_cleanup():
    """Creating a session cleans up expired ones."""
    mgr = SessionManager(timeout_mins=0)  # Immediate expiry
    s1 = mgr.create()
    s1_id = s1.id

    # s1 is now expired. Creating s2 should clean it up.
    s2 = mgr.create()
    assert s2.id != s1_id
    assert mgr.get(s1_id) is None
    # Verify s1 is actually gone from internal dict (not just lazily expired on get)
    assert s1_id not in mgr._sessions
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_sessions.py::test_create_triggers_cleanup -v`
Expected: FAIL — `s1_id` is still in `mgr._sessions` (it's only lazily removed on `get`)

**Step 3: Implement**

In `src/ggsql_rest/_sessions.py`, add `self.cleanup_expired()` as the first line of `SessionManager.create()`:

```python
    def create(self) -> Session:
        """Create a new session."""
        self.cleanup_expired()
        session_id = uuid.uuid4().hex
        session = Session(session_id, self._timeout_mins)
        self._sessions[session_id] = session
        return session
```

**Step 4: Run all session tests**

Run: `uv run pytest tests/test_sessions.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/ggsql_rest/_sessions.py tests/test_sessions.py
git commit -m "fix: cleanup expired sessions on every create"
```

---

### Task 4: Deduplicate version — use `importlib.metadata`

**Files:**
- Modify: `src/ggsql_rest/__init__.py`
- Test: `tests/test_app.py`

**Step 1: Write the failing test**

Add to `tests/test_app.py`:

```python
def test_version_matches_pyproject():
    """Version in __init__ should come from pyproject.toml via metadata."""
    from importlib.metadata import version
    import ggsql_rest

    assert ggsql_rest.__version__ == version("ggsql-rest")
```

**Step 2: Run test to verify it passes (it might already work)**

Run: `uv run pytest tests/test_app.py::test_version_matches_pyproject -v`

This test might pass already since `__version__` is hardcoded to the same value. The goal is to make the single source of truth `pyproject.toml`.

**Step 3: Implement**

Replace `src/ggsql_rest/__init__.py`:

```python
"""ggsql REST API server with SQLAlchemy backend support."""

from importlib.metadata import version

from ._app import create_app
from ._connections import ConnectionRegistry

__version__ = version("ggsql-rest")
__all__ = ["create_app", "ConnectionRegistry"]
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_app.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/ggsql_rest/__init__.py tests/test_app.py
git commit -m "fix: derive __version__ from pyproject.toml via importlib.metadata"
```

---

### Task 5: Consolidate dev dependencies in pyproject.toml

**Files:**
- Modify: `pyproject.toml`

No TDD needed — this is config only.

**Step 1: Update pyproject.toml**

Replace the `[project.optional-dependencies]` and `[dependency-groups]` sections. Remove the `[project.optional-dependencies]` block entirely. Update the `[dependency-groups]` block to include all dev deps:

```toml
[dependency-groups]
dev = [
    "httpx>=0.28.1",
    "pytest>=9.0.2",
    "anyio[trio]>=4.0",
    "ruff>=0.1",
]
```

This:
- Removes redundant `[project.optional-dependencies]` (test and dev)
- Adds `anyio[trio]` (item 11 — missing test dependency)
- Adds `ruff` to the dependency group (was only in optional-dependencies)

**Step 2: Sync and verify**

Run: `uv sync`
Run: `uv run pytest -v`
Run: `uv run ruff check src/ tests/`
Run: `uv run ruff format --check src/ tests/`
Expected: All pass

**Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore: consolidate dev deps into dependency-groups, add anyio and ruff"
```

---

### Task 6: Remove dead module-level sentinels

**Files:**
- Modify: `src/ggsql_rest/_routes/_sessions.py`
- Modify: `src/ggsql_rest/_routes/_query.py`

No TDD needed — removing dead code.

**Step 1: Remove sentinels**

In `src/ggsql_rest/_routes/_sessions.py`, remove lines 15-16:

```python
# Dependency placeholder - will be overridden by app factory
_session_manager: SessionManager | None = None
```

And simplify `get_session_manager` to just raise (the body checking `_session_manager` is unreachable since it's always `None`):

```python
def get_session_manager() -> SessionManager:
    """Dependency placeholder — overridden by app factory."""
    raise RuntimeError("SessionManager not initialized")
```

Also remove `SessionManager` from the import on line 11 since it's now only used as a type annotation in the `Depends` calls (but actually it IS used in the return type and Depends default, so keep it).

In `src/ggsql_rest/_routes/_query.py`, remove lines 19-20:

```python
# Dependency placeholder - will be overridden by app factory
_registry: ConnectionRegistry | None = None
```

And simplify `get_registry`:

```python
def get_registry() -> ConnectionRegistry:
    """Dependency placeholder — overridden by app factory."""
    raise RuntimeError("ConnectionRegistry not initialized")
```

**Step 2: Run full test suite**

Run: `uv run pytest -v`
Expected: All PASS (these sentinels were never read)

**Step 3: Commit**

```bash
git add src/ggsql_rest/_routes/_sessions.py src/ggsql_rest/_routes/_query.py
git commit -m "chore: remove dead module-level sentinel variables"
```

---

### Task 7: Final verification

**Step 1: Run full test suite**

Run: `uv run pytest -v`
Expected: All PASS

**Step 2: Run linter**

Run: `uv run ruff check src/ tests/`
Expected: Clean

**Step 3: Run formatter**

Run: `uv run ruff format --check src/ tests/`
Expected: Clean
