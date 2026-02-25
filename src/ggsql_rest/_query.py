import json
import uuid
from typing import Any

import polars as pl
from sqlalchemy import Engine, text

from ggsql import validate, VegaLiteWriter

from ._sessions import Session

try:
    import connectorx as cx  # noqa: F401

    HAS_CONNECTORX = True
except ImportError:
    HAS_CONNECTORX = False


_DEFAULT_GGSQL_MAX_ROWS = 500_000


def fetch_remote_into_duckdb(
    engine: Engine,
    sql: str,
    session: Session,
    table_name: str,
    max_rows: int | None = None,
    adbc_conn: Any | None = None,
) -> bool:
    """Fetch remote SQL results and register them in session's DuckDB.

    When ADBC or connectorx is available, fetches as a single Arrow DataFrame (zero-copy).
    Otherwise, streams chunks via server-side cursor to bound memory usage.

    Returns True if result was truncated to max_rows, False otherwise.
    """
    truncated = False

    # Fetch one extra row to detect truncation
    fetch_limit = max_rows + 1 if max_rows is not None else None

    # Build LIMIT SQL once upfront
    if fetch_limit is not None:
        fetch_sql = f"SELECT * FROM ({sql}) AS _limited LIMIT {fetch_limit}"
    else:
        fetch_sql = sql

    # 1. ADBC path (Arrow Flight)
    if adbc_conn is not None:
        try:
            df = execute_via_adbc(adbc_conn, fetch_sql, row_limit=None)
            if max_rows is not None and len(df) > max_rows:
                truncated = True
                df = df.head(max_rows)
            session.duckdb.register(table_name, df)
            return truncated
        except Exception:
            pass  # Fall through

    # 2. ConnectorX path
    cx_url = connectorx_supported_url(engine) if HAS_CONNECTORX else None

    if cx_url is not None:
        try:
            df = execute_via_connectorx(cx_url, fetch_sql, row_limit=None)

            # Check for truncation
            if max_rows is not None and len(df) > max_rows:
                truncated = True
                df = df.head(max_rows)

            session.duckdb.register(table_name, df)
            return truncated
        except Exception:
            pass  # Fall through to cursor streaming path

    # Cursor path: stream chunks into DuckDB to bound memory
    batch_size = 10_000
    total_rows = 0
    with engine.connect() as conn:
        conn = conn.execution_options(stream_results=True)
        result = conn.execute(text(sql))
        columns = list(result.keys())

        created = False

        while True:
            # If we've hit the limit, stop fetching
            if max_rows is not None and total_rows >= max_rows:
                truncated = True
                break

            rows = result.fetchmany(batch_size)
            if not rows:
                break

            # Check if this chunk would exceed max_rows
            if max_rows is not None and total_rows + len(rows) > max_rows:
                # Take only what we need
                rows = rows[: max_rows - total_rows]
                truncated = True

            data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
            chunk_df = pl.DataFrame(data)
            total_rows += len(rows)

            if not created:
                session.duckdb.register(table_name, chunk_df)
                created = True
            else:
                session.duckdb.register("__chunk__", chunk_df)
                session.duckdb.execute_sql(
                    f'INSERT INTO "{table_name}" SELECT * FROM __chunk__'
                )

            # Stop if we hit the limit
            if max_rows is not None and total_rows >= max_rows:
                truncated = True
                break

        if not created:
            # Empty result — register empty DataFrame with correct columns
            session.duckdb.register(
                table_name, pl.DataFrame({col: [] for col in columns})
            )

    return truncated


def execute_ggsql(
    query: str,
    session: Session,
    engine: Engine | None = None,
    max_rows: int | None = None,
    adbc_conn: Any | None = None,
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
    truncated = False

    if engine is not None and sql_portion.strip():
        # Apply default max_rows for remote queries
        effective_max_rows = (
            max_rows if max_rows is not None else _DEFAULT_GGSQL_MAX_ROWS
        )
        table_name = f"__remote_result_{uuid.uuid4().hex[:8]}__"
        truncated = fetch_remote_into_duckdb(
            engine,
            sql_portion,
            session,
            table_name,
            effective_max_rows,
            adbc_conn=adbc_conn,
        )
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
            "truncated": truncated,
        },
    }


def connectorx_supported_url(engine: Engine) -> str | None:
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


def execute_via_adbc(
    adbc_conn: Any,
    sql: str,
    row_limit: int | None,
) -> pl.DataFrame:
    """Arrow-native transfer via ADBC."""
    if row_limit is not None:
        sql = f"SELECT * FROM ({sql}) AS _limited LIMIT {row_limit}"
    cursor = adbc_conn.cursor()
    cursor.execute(sql)
    arrow_table = cursor.fetch_arrow_table()
    result = pl.from_arrow(arrow_table)
    # PyArrow Table always converts to DataFrame, but pyright doesn't know this
    assert isinstance(result, pl.DataFrame)
    return result


def execute_remote(
    engine: Engine,
    sql: str,
    max_rows: int | None = None,
    timeout_seconds: int | None = None,
    adbc_conn: Any | None = None,
) -> pl.DataFrame:
    """Execute SQL on remote database, return as Polars DataFrame.

    Tries in order: ADBC (Arrow Flight), connectorx, cursor fallback.

    If max_rows is provided, fetches max_rows + 1 rows for truncation detection.
    """
    # Fetch one extra row so callers can detect truncation
    row_limit = max_rows + 1 if max_rows is not None else None

    # 1. ADBC path
    if adbc_conn is not None:
        try:
            return execute_via_adbc(adbc_conn, sql, row_limit)
        except Exception:
            pass  # Fall through to connectorx

    # 2. ConnectorX path
    cx_url = connectorx_supported_url(engine) if HAS_CONNECTORX else None
    if cx_url is not None:
        try:
            return execute_via_connectorx(cx_url, sql, row_limit)
        except Exception:
            pass  # Fall through to cursor

    # 3. Cursor fallback
    return execute_via_cursor(engine, sql, row_limit, timeout_seconds)


def execute_via_connectorx(
    url: str,
    sql: str,
    row_limit: int | None,
) -> pl.DataFrame:
    """Fast path: Arrow-native transfer via connectorx."""
    if row_limit is not None:
        sql = f"SELECT * FROM ({sql}) AS _limited LIMIT {row_limit}"
    return pl.read_database_uri(sql, url, engine="connectorx")


def execute_via_cursor(
    engine: Engine,
    sql: str,
    row_limit: int | None,
    timeout_seconds: int | None,
) -> pl.DataFrame:
    """Fallback: cursor-based read via SQLAlchemy."""
    with engine.connect() as conn:
        opts: dict[str, Any] = {}
        if row_limit is not None:
            opts["stream_results"] = True
        if timeout_seconds is not None:
            opts["timeout"] = timeout_seconds
        if opts:
            conn = conn.execution_options(**opts)

        result = conn.execute(text(sql))
        columns = list(result.keys())

        if row_limit is not None:
            rows = result.fetchmany(row_limit)
        else:
            rows = result.fetchall()

        data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        return pl.DataFrame(data)


def execute_sql(
    query: str,
    session: Session,
    engine: Engine | None = None,
    max_rows: int = 10000,
    timeout_seconds: int | None = None,
    adbc_conn: Any | None = None,
) -> dict[str, Any]:
    if engine is not None:
        df = execute_remote(
            engine,
            query,
            max_rows=max_rows,
            timeout_seconds=timeout_seconds,
            adbc_conn=adbc_conn,
        )
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
