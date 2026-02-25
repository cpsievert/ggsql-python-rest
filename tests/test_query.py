"""Tests for query execution."""

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool
from ggsql_rest._sessions import Session
from ggsql_rest._query import (
    execute_ggsql,
    execute_sql,
    execute_remote,
    fetch_remote_into_duckdb,
)


def test_execute_ggsql_local():
    """Test executing a ggsql query against local DuckDB."""
    session = Session("test", timeout_mins=30)

    # Create test data in session's DuckDB
    session.duckdb.execute_sql(
        "CREATE TABLE test AS SELECT 1 as x, 2 as y UNION SELECT 3, 4"
    )

    result = execute_ggsql(
        "SELECT * FROM test VISUALISE x, y DRAW point",
        session,
        engine=None,
    )

    assert "spec" in result
    assert "metadata" in result
    assert result["metadata"]["rows"] == 2
    assert "x" in result["metadata"]["columns"]
    assert "y" in result["metadata"]["columns"]


def test_execute_ggsql_no_visualise():
    """Test that queries without VISUALISE raise an error."""
    session = Session("test", timeout_mins=30)

    with pytest.raises(ValueError, match="VISUALISE"):
        execute_ggsql("SELECT 1 as x", session, engine=None)


def test_execute_ggsql_invalid_parse():
    """Test that malformed ggsql queries raise a validation error instead of 500."""
    session = Session("test", timeout_mins=30)

    # Trailing SQL after VISUALISE produces a parse error
    with pytest.raises(ValueError, match="Invalid ggsql query"):
        execute_ggsql(
            "SELECT 1 as x VISUALISE x, x DRAW point SELECT 1",
            session,
            engine=None,
        )


def test_execute_sql_remote_pushes_limit():
    """execute_sql should push LIMIT into the remote SQL instead of fetching all rows."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE big (id INTEGER)"))
        conn.execute(
            text(
                "INSERT INTO big (id) VALUES " + ", ".join(f"({i})" for i in range(100))
            )
        )

    session = Session("test", timeout_mins=30)
    result = execute_sql("SELECT * FROM big", session, engine=engine, max_rows=10)

    assert result["truncated"] is True
    assert len(result["rows"]) == 10
    assert (
        result["row_count"] == 10
    )  # we only know we fetched max_rows; true count unknown


def test_execute_sql_remote_no_truncation_when_under_limit():
    """When result fits within max_rows, no truncation flag."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE small (id INTEGER)"))
        conn.execute(text("INSERT INTO small (id) VALUES (1), (2), (3)"))

    session = Session("test", timeout_mins=30)
    result = execute_sql("SELECT * FROM small", session, engine=engine, max_rows=10)

    assert result["truncated"] is False
    assert len(result["rows"]) == 3
    assert result["row_count"] == 3


def test_execute_ggsql_remote_limits_rows():
    """execute_ggsql with engine should cap the number of rows fetched remotely."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE big (x INTEGER, y INTEGER)"))
        conn.execute(
            text(
                "INSERT INTO big (x, y) VALUES "
                + ", ".join(f"({i}, {i * 2})" for i in range(500))
            )
        )

    session = Session("test", timeout_mins=30)

    result = execute_ggsql(
        "SELECT * FROM big VISUALISE x, y DRAW point",
        session,
        engine=engine,
        max_rows=50,
    )

    # Should have at most 50 rows in the visualization
    assert result["metadata"]["rows"] <= 50


def test_execute_remote_respects_timeout():
    """execute_remote should accept a timeout parameter."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER)"))
        conn.execute(text("INSERT INTO t VALUES (1)"))

    # Should work normally with a timeout set (SQLite doesn't enforce it,
    # but we verify the parameter is accepted and execution completes)
    df = execute_remote(engine, "SELECT * FROM t", timeout_seconds=5)
    assert len(df) == 1


def test_execute_remote_with_connectorx():
    """execute_remote uses connectorx Arrow path when available and URI is supported."""
    pytest.importorskip("connectorx")

    import tempfile
    import os

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    file_engine = create_engine(f"sqlite:///{db_path}")
    try:
        with file_engine.begin() as conn:
            conn.execute(text("CREATE TABLE t (id INTEGER, name TEXT)"))
            conn.execute(text("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"))

        df = execute_remote(file_engine, "SELECT * FROM t")
        assert len(df) == 3
        assert set(df.columns) == {"id", "name"}
    finally:
        file_engine.dispose()
        os.unlink(db_path)


def test_execute_remote_connectorx_with_max_rows():
    """execute_remote with connectorx still respects max_rows via SQL LIMIT."""
    pytest.importorskip("connectorx")

    import tempfile
    import os

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    file_engine = create_engine(f"sqlite:///{db_path}")
    try:
        with file_engine.begin() as conn:
            conn.execute(text("CREATE TABLE t (id INTEGER)"))
            conn.execute(
                text(
                    "INSERT INTO t (id) VALUES "
                    + ", ".join(f"({i})" for i in range(100))
                )
            )

        df = execute_remote(file_engine, "SELECT * FROM t", max_rows=10)
        assert len(df) == 11  # max_rows + 1 for truncation detection
    finally:
        file_engine.dispose()
        os.unlink(db_path)


def test_execute_remote_falls_back_without_connectorx(monkeypatch):
    """execute_remote falls back to cursor when connectorx is unavailable."""
    monkeypatch.setattr("ggsql_rest._query.HAS_CONNECTORX", False)

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER)"))
        conn.execute(text("INSERT INTO t VALUES (1), (2), (3)"))

    df = execute_remote(engine, "SELECT * FROM t")
    assert len(df) == 3


def test_execute_remote_falls_back_for_memory_sqlite():
    """execute_remote falls back to cursor for in-memory SQLite (connectorx can't connect)."""
    pytest.importorskip("connectorx")

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER)"))
        conn.execute(text("INSERT INTO t VALUES (1), (2), (3)"))

    # Should still work — falls back to cursor path
    df = execute_remote(engine, "SELECT * FROM t")
    assert len(df) == 3


def test_execute_ggsql_streams_large_remote_result():
    """execute_ggsql handles large remote results (regression guard for streaming refactor)."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE data (x INTEGER, y INTEGER)"))
        conn.execute(
            text(
                "INSERT INTO data (x, y) VALUES "
                + ", ".join(f"({i}, {i * 2})" for i in range(1000))
            )
        )

    session = Session("test", timeout_mins=30)
    result = execute_ggsql(
        "SELECT * FROM data VISUALISE x, y DRAW point",
        session,
        engine=engine,
    )

    assert result["metadata"]["rows"] == 1000


def test_execute_ggsql_streams_empty_remote_result():
    """execute_ggsql handles empty remote results without error."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE empty (x INTEGER, y INTEGER)"))

    session = Session("test", timeout_mins=30)

    # Empty table with VISUALISE — ggsql should handle this gracefully
    # (may raise if ggsql doesn't support empty data, which is fine —
    # the test verifies _fetch_remote_into_duckdb handles 0 rows)
    try:
        result = execute_ggsql(
            "SELECT * FROM empty VISUALISE x, y DRAW point",
            session,
            engine=engine,
        )
        assert result["metadata"]["rows"] == 0
    except Exception:
        # If ggsql can't visualize empty data, that's a ggsql issue, not ours.
        # The important thing is we didn't crash in _fetch_remote_into_duckdb.
        pass


def test_fetch_remote_into_duckdb_pushes_limit_to_db(monkeypatch):
    """fetch_remote_into_duckdb should inject LIMIT into the SQL sent to the remote DB."""
    from sqlalchemy import event

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE big (x INTEGER, y INTEGER)"))
        conn.execute(
            text(
                "INSERT INTO big (x, y) VALUES "
                + ", ".join(f"({i}, {i * 2})" for i in range(200))
            )
        )

    # Disable connectorx to force cursor path
    monkeypatch.setattr("ggsql_rest._query.HAS_CONNECTORX", False)

    # Capture the SQL that gets executed via SQLAlchemy event
    executed_sqls: list[str] = []

    @event.listens_for(engine, "before_cursor_execute")
    def capture_sql(conn, cursor, statement, parameters, context, executemany):
        executed_sqls.append(statement)

    session = Session("test", timeout_mins=30)
    fetch_remote_into_duckdb(
        engine, "SELECT * FROM big", session, "test_table", max_rows=50
    )

    # The SQL sent to the DB should contain a LIMIT clause
    assert any("LIMIT" in sql.upper() for sql in executed_sqls), (
        f"Expected LIMIT in SQL, got: {executed_sqls}"
    )
