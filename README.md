# ggsql-rest

REST API server for [ggsql](https://github.com/posit-dev/ggsql) with SQLAlchemy backend support.

## Overview

ggsql-rest provides an HTTP interface for executing ggsql queries. It implements a **hybrid execution model** where SQL runs on remote databases (via SQLAlchemy) and VISUALISE clauses run locally (via DuckDB), enabling visualization of data from enterprise databases.

### Key features

- **Session management**: Isolated DuckDB instances per user session with automatic expiration
- **File upload**: Upload CSV, Parquet, JSON, JSONL, or NDJSON files to session databases
- **Hybrid execution**: SQL on remote databases, VISUALISE locally
- **Schema introspection**: Discover tables and columns across local and remote databases, with optional statistics
- **Connection registry**: Named database connections with per-user caching and LRU eviction
- **Pure SQL endpoint**: Execute SQL queries without visualization
- **CLI**: Run the server from the command line with YAML-based connection configuration

## Installation

```bash
pip install ggsql-rest
```

Or for development:

```bash
git clone https://github.com/posit-dev/ggsql-python-rest.git
cd ggsql-python-rest
uv sync
```

## Quick start

### CLI

The simplest way to run the server is with the `ggsql-rest` CLI:

```bash
# Start with sample data (products, sales, employees tables)
ggsql-rest --load-sample-data

# Load your own data files (CSV, Parquet, JSON)
ggsql-rest --load-data sales.csv --load-data products.parquet

# Combine sample data, custom files, and remote database connections
ggsql-rest --load-sample-data --load-data extra.csv --connections config.yaml

# Customize host, port, and CORS
ggsql-rest --port 3334 --host 0.0.0.0 --cors-origins http://localhost:3000
```

CLI options:

| Flag | Default | Description |
|------|---------|-------------|
| `--load-sample-data` | off | Load sample data (products, sales, employees) into all sessions |
| `--load-data FILE` | — | Load a data file (CSV, Parquet, JSON) into all sessions. Repeatable. |
| `--connections` | — | Path to YAML connection config file |
| `--host` | `127.0.0.1` | Host to bind to |
| `--port` | `8000` | Port to listen on |
| `--cors-origins` | — | Space-separated list of allowed CORS origins |

Data loaded via `--load-sample-data` and `--load-data` is seeded into every new session, so all users see the same base tables. Users can also upload additional files per-session via the upload endpoint.

### YAML connection configuration

The `--connections` flag accepts a YAML file defining named database connections:

```yaml
connections:
  warehouse:
    url: "postgresql://user:pass@host:5432/db"
    pool_size: 5
    pool_pre_ping: true
    connect_args:
      sslmode: require

  local_sqlite:
    url: "sqlite:///path/to/db.sqlite"
```

Each connection requires a `url` key. Any other keys are passed as keyword arguments to SQLAlchemy's `create_engine()`.

### Python API

For programmatic use:

```python
from ggsql_rest import create_app, ConnectionRegistry

registry = ConnectionRegistry()
app = create_app(registry)
```

With a remote database connection:

```python
from sqlalchemy import create_engine
from ggsql_rest import create_app, ConnectionRegistry

registry = ConnectionRegistry()
registry.register("warehouse", lambda req: create_engine("postgresql://..."))

app = create_app(registry, cors_origins=["http://localhost:3000"])
```

Or load connections from YAML in Python:

```python
from ggsql_rest import create_app, load_connections_from_yaml

registry = load_connections_from_yaml("config.yaml")
app = create_app(registry)
```

## API endpoints

All endpoints are served under the `/api/v1` prefix.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `POST` | `/sessions` | Create a new session |
| `DELETE` | `/sessions/{id}` | Delete a session |
| `GET` | `/sessions/{id}/tables` | List tables in session |
| `POST` | `/sessions/{id}/upload` | Upload a data file |
| `GET` | `/sessions/{id}/schema` | Schema introspection (local + remote tables) |
| `POST` | `/sessions/{id}/query` | Execute a ggsql query |
| `POST` | `/sessions/{id}/sql` | Execute a pure SQL query |

### Response format

All responses use a standard envelope:

```json
{"status": "success", "data": { ... }}
```

```json
{"status": "error", "error": {"type": "InvalidRequest", "message": "..."}}
```

Response fields use camelCase (e.g., `sessionId`, `tableName`, `rowCount`).

### Schema introspection

`GET /sessions/{id}/schema` returns column metadata for all available tables (both uploaded files and remote database tables). Pass `?include_stats=true` to include:

- **Numeric columns**: `minValue`, `maxValue`
- **Text columns** (up to 20 distinct values): `categoricalValues`

### File upload

`POST /sessions/{id}/upload` accepts multipart/form-data. Supported formats: `.csv`, `.parquet`, `.json`, `.jsonl`, `.ndjson`. The uploaded file is registered as a table in the session's DuckDB instance.

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

## Development

```bash
uv sync              # Install dependencies
uv run pytest        # Run tests
uv run pyright       # Type check
uv run ruff format   # Format code
uv run ruff check    # Lint
```
