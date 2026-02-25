"""Tests for query execution."""

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool
from ggsql_rest._sessions import Session
from ggsql_rest._query import execute_ggsql, execute_sql


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
        conn.execute(text(
            "INSERT INTO big (id) VALUES " + ", ".join(f"({i})" for i in range(100))
        ))

    session = Session("test", timeout_mins=30)
    result = execute_sql("SELECT * FROM big", session, engine=engine, max_rows=10)

    assert result["truncated"] is True
    assert len(result["rows"]) == 10
    assert result["row_count"] == 10  # we only know we fetched max_rows; true count unknown


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
        conn.execute(text(
            "INSERT INTO big (x, y) VALUES " +
            ", ".join(f"({i}, {i*2})" for i in range(500))
        ))

    session = Session("test", timeout_mins=30)

    result = execute_ggsql(
        "SELECT * FROM big VISUALISE x, y DRAW point",
        session,
        engine=engine,
        max_rows=50,
    )

    # Should have at most 50 rows in the visualization
    assert result["metadata"]["rows"] <= 50
