import json
import uuid
from typing import Any

import polars as pl
from sqlalchemy import Engine, text

from ggsql import validate, VegaLiteWriter

from ._sessions import Session

try:
    import connectorx as _cx  # noqa: F401

    _HAS_CONNECTORX = True
except ImportError:
    _HAS_CONNECTORX = False


def _fetch_remote_into_duckdb(
    engine: Engine,
    sql: str,
    session: Session,
    table_name: str,
    max_rows: int | None = None,
) -> None:
    """Fetch remote SQL results and register them in session's DuckDB.

    When connectorx is available, fetches as a single Arrow DataFrame (zero-copy).
    Otherwise, streams partitions via server-side cursor to bound memory usage.
    """
    cx_url = _connectorx_supported_url(engine) if _HAS_CONNECTORX else None

    if cx_url is not None:
        try:
            df = _execute_via_connectorx(cx_url, sql, max_rows)
            if max_rows is not None and len(df) > max_rows:
                df = df.head(max_rows)
            session.duckdb.register(table_name, df)
            return
        except Exception:
            pass  # Fall through to cursor streaming path

    # Cursor path: stream partitions into DuckDB to bound memory
    with engine.connect() as conn:
        conn = conn.execution_options(stream_results=True)
        result = conn.execute(text(sql))
        columns = list(result.keys())

        created = False
        total_rows = 0

        for partition in result.partitions(10_000):
            if max_rows is not None and total_rows >= max_rows:
                break

            data = {col: [row[i] for row in partition] for i, col in enumerate(columns)}
            chunk_df = pl.DataFrame(data)

            if max_rows is not None:
                remaining = max_rows - total_rows
                if len(chunk_df) > remaining:
                    chunk_df = chunk_df.head(remaining)

            if not created:
                session.duckdb.register(table_name, chunk_df)
                created = True
            else:
                session.duckdb.register("__chunk__", chunk_df)
                session.duckdb.execute_sql(
                    f'INSERT INTO "{table_name}" SELECT * FROM __chunk__'
                )

            total_rows += len(chunk_df)

        if not created:
            # Empty result — register empty DataFrame with correct columns
            session.duckdb.register(
                table_name, pl.DataFrame({col: [] for col in columns})
            )


def execute_ggsql(
    query: str,
    session: Session,
    engine: Engine | None = None,
    max_rows: int | None = None,
) -> dict[str, Any]:
    """Execute a ggsql query with hybrid local/remote approach.

    If engine is provided, SQL portion runs on remote database,
    result is registered in session's DuckDB, and VISUALISE
    portion runs locally.
    """
    validated = validate(query)

    # Reject queries with parse errors — these produce corrupted sql() output
    # that can cause 500s on remote databases. Semantic errors (e.g., missing
    # aesthetics) are allowed through since the executor may handle them fine.
    parse_errors = [
        e for e in validated.errors() if e.get("message", "").startswith("Parse error")
    ]
    if parse_errors:
        messages = "; ".join(e["message"] for e in parse_errors)
        raise ValueError(f"Invalid ggsql query: {messages}")

    if not validated.has_visual():
        raise ValueError("Query must contain VISUALISE clause")

    sql_portion = validated.sql()

    if engine is not None and sql_portion.strip():
        table_name = f"__remote_result_{uuid.uuid4().hex[:8]}__"
        _fetch_remote_into_duckdb(engine, sql_portion, session, table_name, max_rows)
        local_query = f"SELECT * FROM {table_name} {validated.visual()}"
    else:
        local_query = query

    spec = session.duckdb.execute(local_query)

    writer = VegaLiteWriter()
    vegalite_json = writer.render(spec)

    return {
        "spec": json.loads(vegalite_json),
        "metadata": {
            "rows": spec.metadata()["rows"],
            "columns": spec.metadata()["columns"],
            "layers": spec.metadata()["layer_count"],
        },
    }


def _connectorx_supported_url(engine: Engine) -> str | None:
    """Return a connectorx-compatible URI string, or None if unsupported.

    Connectorx can't connect to:
    - In-memory SQLite (no URI to connect to from a separate process)
    - Snowflake (use ADBC instead)
    """
    url = str(engine.url)
    if ":memory:" in url:
        return None
    if "snowflake" in url:
        return None
    return url


def execute_remote(
    engine: Engine,
    sql: str,
    max_rows: int | None = None,
    timeout_seconds: int | None = None,
) -> pl.DataFrame:
    """Execute SQL on remote database, return as Polars DataFrame.

    Uses connectorx for Arrow-native transfer when available and the
    engine URL is supported, falling back to cursor-based reads otherwise.
    """
    cx_url = _connectorx_supported_url(engine) if _HAS_CONNECTORX else None

    if cx_url is not None:
        try:
            return _execute_via_connectorx(cx_url, sql, max_rows)
        except Exception:
            pass  # Fall through to cursor path

    return _execute_via_cursor(engine, sql, max_rows, timeout_seconds)


def _execute_via_connectorx(
    url: str,
    sql: str,
    max_rows: int | None,
) -> pl.DataFrame:
    """Fast path: Arrow-native transfer via connectorx."""
    if max_rows is not None:
        sql = f"SELECT * FROM ({sql}) AS _limited LIMIT {max_rows + 1}"
    return pl.read_database_uri(sql, url, engine="connectorx")


def _execute_via_cursor(
    engine: Engine,
    sql: str,
    max_rows: int | None,
    timeout_seconds: int | None,
) -> pl.DataFrame:
    """Fallback: cursor-based read via SQLAlchemy."""
    with engine.connect() as conn:
        opts: dict[str, Any] = {}
        if max_rows is not None:
            opts["stream_results"] = True
        if timeout_seconds is not None:
            opts["timeout"] = timeout_seconds
        if opts:
            conn = conn.execution_options(**opts)

        result = conn.execute(text(sql))
        columns = list(result.keys())

        if max_rows is not None:
            rows = result.fetchmany(max_rows + 1)
        else:
            rows = result.fetchall()

        data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        return pl.DataFrame(data)


def execute_sql(
    query: str,
    session: Session,
    engine: Engine | None = None,
    max_rows: int = 10000,
) -> dict[str, Any]:
    if engine is not None:
        df = execute_remote(engine, query, max_rows=max_rows)
    else:
        df = session.duckdb.execute_sql(query)

    row_count = len(df)
    truncated = row_count > max_rows

    if truncated:
        df = df.head(max_rows)

    return {
        "rows": df.to_dicts(),
        "columns": df.columns,
        "row_count": min(row_count, max_rows),
        "truncated": truncated,
    }
