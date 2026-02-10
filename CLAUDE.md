# ggsql-rest

REST API server for ggsql with SQLAlchemy backend support.

## Architecture

FastAPI server wrapping [ggsql-python](https://github.com/posit-dev/ggsql) bindings. Uses a hybrid execution model: SQL runs on remote databases (via SQLAlchemy), VISUALISE runs locally (via DuckDB).

```
HTTP Client
    |
FastAPI REST API (/api/v1)
    |
+---+---+
|       |
ConnectionRegistry    SessionManager
(Remote DB Engines)   (Isolated DuckDB instances)
|                     |
SQLAlchemy Engine     ggsql.DuckDBReader
|                     |
Remote SQL Query      VISUALISE execution
|                     |
Polars DataFrame ---> DuckDB table
                      |
                      ggsql.VegaLiteWriter
                      |
                      Vega-Lite JSON
```

## Package structure

```
src/ggsql_rest/
  __init__.py          # Public API: create_app, ConnectionRegistry, load_connections_from_yaml
  __main__.py          # CLI entry point (argparse)
  _app.py              # FastAPI app factory
  _config.py           # YAML connection config loading
  _connections.py      # ConnectionRegistry (named DB connections, per-user LRU cache)
  _errors.py           # Error handling + custom exceptions
  _models.py           # Pydantic request/response models (camelCase envelope)
  _query.py            # Query execution (hybrid local/remote)
  _schema.py           # Schema introspection (local + remote, optional stats)
  _sessions.py         # Session management (isolated DuckDB per session, auto-expiry)
  _routes/
    __init__.py
    _health.py         # GET /health
    _query.py          # POST /sessions/{id}/query, /sessions/{id}/sql
    _schema.py         # GET /sessions/{id}/schema
    _sessions.py       # Session CRUD + file upload
```

## Key conventions

- All routes prefixed with `/api/v1`
- Responses use `{"status": "success", "data": ...}` / `{"status": "error", "error": {...}}` envelope
- Response field names are camelCase (Pydantic alias)
- Sessions auto-expire after 30 minutes (configurable via `create_app(session_timeout_mins=...)`)
- ConnectionRegistry caches engines per user (`X-User-Id` header, defaults to "anonymous") with LRU eviction

## Dependencies

- **ggsql**: Python bindings for ggsql (git dep; imports `DuckDBReader`, `validate`, `VegaLiteWriter`)
- **FastAPI**: HTTP framework
- **SQLAlchemy**: Remote database connectivity
- **Polars**: DataFrame operations
- **Pydantic**: Request/response validation
- **PyYAML**: YAML config file loading
- **uvicorn**: ASGI server

## Commands

```bash
uv sync              # Install dependencies
uv run pytest        # Run tests
uv run pyright       # Type check
uv run ruff format   # Format code
uv run ruff check    # Lint
```
