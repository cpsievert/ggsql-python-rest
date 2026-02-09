# ggsql-rest

REST API server for ggsql with SQLAlchemy backend support.

## Architecture

FastAPI server wrapping [ggsql-python](https://github.com/posit-dev/ggsql) bindings. Uses a hybrid execution model: SQL runs on remote databases (via SQLAlchemy), VISUALISE runs locally (via DuckDB).

```
HTTP Client
    |
FastAPI REST API
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
  __init__.py          # Public API: create_app, ConnectionRegistry
  _app.py              # FastAPI app factory
  _connections.py      # ConnectionRegistry (named DB connections)
  _errors.py           # Error handling utilities
  _models.py           # Pydantic request/response models
  _query.py            # Query execution (hybrid local/remote)
  _sessions.py         # Session management (isolated DuckDB per session)
  _routes/
    __init__.py
    _health.py         # GET /health
    _query.py          # POST /sessions/{id}/query, /sessions/{id}/sql
    _sessions.py       # Session CRUD + file upload
```

## Dependencies

- **ggsql**: Python bindings for ggsql (imports `DuckDBReader`, `validate`, `VegaLiteWriter`)
- **FastAPI**: HTTP framework
- **SQLAlchemy**: Remote database connectivity
- **Polars**: DataFrame operations
- **Pydantic**: Request/response validation

## Commands

```bash
uv sync              # Install dependencies
uv run pytest        # Run tests
uv run ruff format   # Format code
uv run ruff check    # Lint
```
