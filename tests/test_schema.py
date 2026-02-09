"""Tests for schema extraction."""

import polars as pl
from ggsql import DuckDBReader

from ggsql_rest._schema import get_local_table_schema


def _make_session_duckdb_with_table() -> tuple[DuckDBReader, str]:
    """Create a DuckDB instance with a test table."""
    duckdb = DuckDBReader("duckdb://memory")
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "score": [85.5, 92.0, 78.3],
    })
    duckdb.register("test_table", df)
    return duckdb, "test_table"


def test_get_local_table_schema_basic():
    duckdb, table_name = _make_session_duckdb_with_table()
    schema = get_local_table_schema(duckdb, table_name, include_stats=False)

    assert schema.table_name == "test_table"
    assert schema.connection is None
    assert len(schema.columns) == 3

    col_names = [c.column_name for c in schema.columns]
    assert "id" in col_names
    assert "name" in col_names
    assert "score" in col_names

    # No stats when include_stats=False
    for col in schema.columns:
        assert col.min_value is None
        assert col.max_value is None
        assert col.categorical_values is None


def test_get_local_table_schema_with_stats():
    duckdb, table_name = _make_session_duckdb_with_table()
    schema = get_local_table_schema(duckdb, table_name, include_stats=True)

    # Find numeric column - should have min/max
    score_col = next(c for c in schema.columns if c.column_name == "score")
    assert score_col.min_value is not None
    assert score_col.max_value is not None

    # Find text column with <= 20 distinct values → categorical
    name_col = next(c for c in schema.columns if c.column_name == "name")
    assert name_col.categorical_values is not None
    assert set(name_col.categorical_values) == {"Alice", "Bob", "Charlie"}


def test_get_local_table_schema_non_categorical_text():
    """Text columns with > 20 distinct values should not have categoricalValues."""
    duckdb = DuckDBReader("duckdb://memory")
    df = pl.DataFrame({"label": [f"item_{i}" for i in range(25)]})
    duckdb.register("big_table", df)

    schema = get_local_table_schema(duckdb, "big_table", include_stats=True)
    label_col = next(c for c in schema.columns if c.column_name == "label")
    assert label_col.categorical_values is None
