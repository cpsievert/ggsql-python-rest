# ggsql-rest

REST API server for [ggsql](https://github.com/posit-dev/ggsql) with SQLAlchemy backend support.

## Overview

ggsql-rest provides an HTTP interface for executing ggsql queries. It implements a **hybrid execution model** where SQL runs on remote databases (via SQLAlchemy) and VISUALISE clauses run locally (via DuckDB), enabling visualization of data from enterprise databases.

### Key features

- **Session management**: Isolated DuckDB instances per user session
- **File upload**: Upload CSV, Parquet, or JSON files to session databases
- **Hybrid execution**: SQL on remote databases, VISUALISE locally
- **Connection registry**: Named database connections with request-aware factories
- **Pure SQL endpoint**: Execute SQL queries without visualization

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

## Usage

```python
from ggsql_rest import create_app, ConnectionRegistry

registry = ConnectionRegistry()
app = create_app(registry)
```

### With a remote database connection

```python
from sqlalchemy import create_engine
from ggsql_rest import create_app, ConnectionRegistry

registry = ConnectionRegistry()
registry.register("warehouse", lambda req: create_engine("postgresql://..."))

app = create_app(registry, cors_origins=["http://localhost:3000"])
```

### Running the server

```bash
uv run uvicorn ggsql_rest:app --reload
```

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `POST` | `/sessions` | Create a new session |
| `DELETE` | `/sessions/{id}` | Delete a session |
| `GET` | `/sessions/{id}/tables` | List tables in session |
| `POST` | `/sessions/{id}/upload` | Upload a data file |
| `POST` | `/sessions/{id}/query` | Execute a ggsql query |
| `POST` | `/sessions/{id}/sql` | Execute a pure SQL query |

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
# Run tests
uv run pytest

# Format code
uv run ruff format

# Lint
uv run ruff check
```
