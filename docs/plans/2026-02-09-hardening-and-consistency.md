# Hardening and Consistency Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix the unbounded engine cache, add shutdown cleanup, unify error handling, catch query-layer exceptions properly, and document the SQL trust model.

**Architecture:** Mostly surgical changes to existing modules. The engine cache gets an LRU bound. The lifespan handler gets access to shared state via `app.state`. All route-layer errors converge on `ApiError`. A new "Security" section in the README documents the trust model.

**Tech Stack:** FastAPI, SQLAlchemy, Python `collections.OrderedDict` (for LRU), Pydantic, pytest

---

### Task 1: Bounded engine cache with LRU eviction

**Files:**
- Modify: `src/ggsql_rest/_connections.py`
- Test: `tests/test_connections.py`

**Step 1: Write the failing test**

Add to `tests/test_connections.py`:

```python
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
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_connections.py::test_engine_cache_evicts_lru -v`
Expected: FAIL — `ConnectionRegistry` doesn't accept `max_engines`

**Step 3: Implement LRU-bounded cache**

Replace `_connections.py` implementation:

```python
"""Connection registry for named database connections."""

from collections import OrderedDict
from typing import Callable

from fastapi import Request
from sqlalchemy import Engine


class ConnectionRegistry:
    """Registry for named database connections with request-aware factories."""

    def __init__(self, max_engines: int = 100):
        self._factories: dict[str, Callable[[Request], Engine]] = {}
        self._engines: OrderedDict[tuple[str, str], Engine] = OrderedDict()
        self._max_engines = max_engines

    def register(self, name: str, factory: Callable[[Request], Engine]) -> None:
        """Register a named connection factory."""
        self._factories[name] = factory

    def get_engine(self, name: str, request: Request) -> Engine:
        """Get or create a cached engine by name and user."""
        if name not in self._factories:
            raise KeyError(f"Unknown connection: '{name}'")

        user_id = self._extract_user_id(request)
        cache_key = (name, user_id)

        if cache_key in self._engines:
            self._engines.move_to_end(cache_key)
            return self._engines[cache_key]

        engine = self._factories[name](request)
        self._engines[cache_key] = engine

        if len(self._engines) > self._max_engines:
            _, evicted = self._engines.popitem(last=False)
            evicted.dispose()

        return engine

    def _extract_user_id(self, request: Request) -> str:
        """Extract user ID from request headers."""
        return request.headers.get("X-User-Id", "anonymous")

    def list_connections(self) -> list[str]:
        """List available connection names."""
        return list(self._factories.keys())

    def dispose_all(self) -> None:
        """Dispose all cached engines. Called on shutdown."""
        for engine in self._engines.values():
            engine.dispose()
        self._engines.clear()
```

Note: `extract_user_id` becomes `_extract_user_id` (private). If any external code calls it, check first — but the existing tests are the only callers and they should be updated.

**Step 4: Update tests that call `extract_user_id`**

In `tests/test_connections.py`, rename calls from `registry.extract_user_id(...)` to `registry._extract_user_id(...)` in `test_extract_user_id`.

**Step 5: Run all connection tests**

Run: `uv run pytest tests/test_connections.py -v`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/ggsql_rest/_connections.py tests/test_connections.py
git commit -m "fix: bound engine cache with LRU eviction (max 100)"
```

---

### Task 2: Shutdown cleanup via `app.state`

**Files:**
- Modify: `src/ggsql_rest/_app.py`
- Test: `tests/test_app.py`

**Step 1: Write the failing test**

Add to `tests/test_app.py`:

```python
def test_shutdown_disposes_engines():
    """Verify engine disposal runs on app shutdown."""
    from unittest.mock import patch, MagicMock

    registry = ConnectionRegistry()
    app = create_app(registry)

    # Verify state is set
    assert app.state.registry is registry
    assert app.state.session_manager is not None

    # Patch dispose_all and trigger shutdown via lifespan
    with patch.object(registry, "dispose_all") as mock_dispose:
        with TestClient(app):
            pass  # triggers startup + shutdown
        mock_dispose.assert_called_once()
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_app.py::test_shutdown_disposes_engines -v`
Expected: FAIL — `app.state` has no `registry` attribute

**Step 3: Implement shutdown cleanup**

Update `_app.py`:

```python
"""FastAPI application factory."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ._connections import ConnectionRegistry
from ._sessions import SessionManager
from ._errors import register_error_handlers
from ._routes import _health, _sessions, _query


def _make_lifespan(
    registry: ConnectionRegistry,
    session_manager: SessionManager,
):
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        """Application lifespan handler."""
        app.state.registry = registry
        app.state.session_manager = session_manager
        yield
        registry.dispose_all()

    return lifespan


def create_app(
    registry: ConnectionRegistry,
    session_timeout_mins: int = 30,
    cors_origins: list[str] | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application."""
    session_manager = SessionManager(session_timeout_mins)

    app = FastAPI(
        title="ggsql REST API",
        description="REST API server for ggsql with SQLAlchemy backend support",
        lifespan=_make_lifespan(registry, session_manager),
    )

    # Set up dependency overrides
    app.dependency_overrides[_sessions.get_session_manager] = lambda: session_manager
    app.dependency_overrides[_query.get_registry] = lambda: registry

    # CORS (consumer configurable)
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # Register error handlers
    register_error_handlers(app)

    # Register routes
    app.include_router(_health.router)
    app.include_router(_sessions.router)
    app.include_router(_query.router)

    return app
```

**Step 4: Run all app tests**

Run: `uv run pytest tests/test_app.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/ggsql_rest/_app.py tests/test_app.py
git commit -m "fix: dispose engines on app shutdown via lifespan"
```

---

### Task 3: Unify error handling — replace `HTTPException` with `ApiError`

**Files:**
- Modify: `src/ggsql_rest/_routes/_sessions.py`
- Modify: `src/ggsql_rest/_errors.py` (add `invalid_request` factory)
- Test: `tests/test_routes_sessions.py`
- Test: `tests/test_routes_upload.py`

**Step 1: Write tests asserting the structured error format**

Update `tests/test_routes_sessions.py::test_delete_session_not_found`:

```python
def test_delete_session_not_found():
    app, _ = create_test_app()
    client = TestClient(app)

    response = client.delete("/sessions/nonexistent")
    assert response.status_code == 404
    body = response.json()
    assert body["status"] == "error"
    assert body["error"]["type"] == "SessionNotFound"
```

Update `tests/test_routes_sessions.py::test_list_tables_not_found`:

```python
def test_list_tables_not_found():
    app, _ = create_test_app()
    client = TestClient(app)

    response = client.get("/sessions/nonexistent/tables")
    assert response.status_code == 404
    body = response.json()
    assert body["status"] == "error"
    assert body["error"]["type"] == "SessionNotFound"
```

Update `tests/test_routes_upload.py::test_upload_unsupported_format` to expect:

```python
    body = response.json()
    assert body["status"] == "error"
    assert body["error"]["type"] == "InvalidRequest"
```

Update `tests/test_routes_upload.py::test_upload_session_not_found` to expect:

```python
    body = response.json()
    assert body["status"] == "error"
    assert body["error"]["type"] == "SessionNotFound"
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_routes_sessions.py tests/test_routes_upload.py -v`
Expected: FAIL — responses currently use `{"detail": "..."}` format

**Step 3: Add `invalid_request` factory to `_errors.py`**

```python
def invalid_request(message: str) -> ApiError:
    """Create a generic 400 bad request error."""
    return ApiError(400, "InvalidRequest", message)
```

**Step 4: Replace `HTTPException` with `ApiError` in `_routes/_sessions.py`**

Replace imports at top:

```python
from .._errors import session_not_found, invalid_request
```

Replace all `raise HTTPException(404, ...)` with `raise session_not_found(session_id)`.
Replace `raise HTTPException(400, ...)` with `raise invalid_request(message)`.

The `get_session` dependency, `delete_session`, and `upload_file` handlers all need updating.

Important: The test apps in route tests need error handlers registered. Add `register_error_handlers(app)` in each test's `create_test_app()`.

**Step 5: Run tests**

Run: `uv run pytest tests/test_routes_sessions.py tests/test_routes_upload.py -v`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/ggsql_rest/_errors.py src/ggsql_rest/_routes/_sessions.py tests/test_routes_sessions.py tests/test_routes_upload.py
git commit -m "fix: unify error responses on ApiError, remove HTTPException usage"
```

---

### Task 4: Catch `ValueError`/`KeyError` in query routes as 400s

**Files:**
- Modify: `src/ggsql_rest/_errors.py` (add exception handlers)
- Test: `tests/test_routes_query.py`

**Step 1: Write failing tests**

Add to `tests/test_routes_query.py`:

```python
@pytest.mark.anyio
async def test_query_without_visualise_returns_400():
    app, session_mgr, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post("/sessions")
        session_id = create_resp.json()["session_id"]

        response = await client.post(
            f"/sessions/{session_id}/query",
            json={"query": "SELECT 1 AS x"},
        )
        assert response.status_code == 400
        body = response.json()
        assert body["status"] == "error"


@pytest.mark.anyio
async def test_query_unknown_connection_returns_400():
    app, session_mgr, _ = create_test_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post("/sessions")
        session_id = create_resp.json()["session_id"]

        response = await client.post(
            f"/sessions/{session_id}/query",
            json={"query": "SELECT 1 VISUALISE x DRAW point", "connection": "nope"},
        )
        assert response.status_code == 400
        body = response.json()
        assert body["status"] == "error"
        assert body["error"]["type"] == "ConnectionNotFound"
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_routes_query.py::test_query_without_visualise_returns_400 tests/test_routes_query.py::test_query_unknown_connection_returns_400 -v`
Expected: FAIL — currently these return 500

**Step 3: Register exception handlers for `ValueError` and `KeyError`**

Update `_errors.py::register_error_handlers` to also handle these:

```python
def register_error_handlers(app: FastAPI) -> None:
    """Register error handlers on the FastAPI app."""

    @app.exception_handler(ApiError)
    async def handle_api_error(request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "status": "error",
                "error": {"type": exc.error_type, "message": exc.message},
            },
        )

    @app.exception_handler(ValueError)
    async def handle_value_error(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "error": {"type": "InvalidQuery", "message": str(exc)},
            },
        )

    @app.exception_handler(KeyError)
    async def handle_key_error(request: Request, exc: KeyError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "error": {"type": "ConnectionNotFound", "message": str(exc)},
            },
        )
```

Also make sure `create_test_app()` in `tests/test_routes_query.py` calls `register_error_handlers(app)`.

**Step 4: Run tests**

Run: `uv run pytest tests/test_routes_query.py -v`
Expected: All PASS

**Step 5: Run full test suite**

Run: `uv run pytest -v`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/ggsql_rest/_errors.py tests/test_routes_query.py
git commit -m "fix: return 400 for ValueError/KeyError instead of 500"
```

---

### Task 5: Document the SQL trust model

**Files:**
- Modify: `README.md`

**Step 1: Read current README**

Read `README.md` to understand existing structure.

**Step 2: Add Security section to README**

Add a "Security" section (after the API section or at the end) with the following content:

```markdown
## Security

### SQL execution trust model

The `/sessions/{id}/sql` and `/sessions/{id}/query` endpoints execute SQL
provided by the client. **ggsql-rest does not restrict or sanitize SQL
statements.** It is the deployer's responsibility to ensure that:

1. **Access control** is enforced upstream (e.g., authentication middleware,
   reverse proxy, or network-level restrictions) so that only authorized users
   can reach the API.
2. **Database permissions** are scoped appropriately. The connection factory
   passed to `ConnectionRegistry.register()` should return engines connected as
   a database user with minimal privileges (typically read-only). For example:

   ```python
   from sqlalchemy import create_engine

   registry = ConnectionRegistry()
   registry.register(
       "warehouse",
       # Use a read-only database user
       lambda req: create_engine("postgresql://readonly_user:pw@host/db"),
   )
   ```

3. **Row limits** apply to the `/sql` endpoint (default 10,000 rows), but no
   query timeout or cost guard is enforced. Consider setting statement timeouts
   at the database level for untrusted workloads.
```

**Step 3: Commit**

```bash
git add README.md
git commit -m "docs: document SQL execution trust model"
```

---

### Task 6: Final verification

**Step 1: Run full test suite**

Run: `uv run pytest -v`
Expected: All PASS

**Step 2: Run linter**

Run: `uv run ruff check src/ tests/`
Expected: No errors

**Step 3: Run formatter**

Run: `uv run ruff format --check src/ tests/`
Expected: No reformatting needed (or fix if needed)
