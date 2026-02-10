"""Tests for session management."""

from datetime import timezone

import polars as pl

from ggsql_rest._sessions import Session, SessionManager


def test_session_creation():
    session = Session("test123", timeout_mins=30)
    assert session.id == "test123"
    assert session.tables == []
    assert not session.is_expired()


def test_session_touch():
    session = Session("test123", timeout_mins=30)
    first_access = session.last_accessed
    session.touch()
    assert session.last_accessed >= first_access


def test_session_expiry():
    session = Session("test123", timeout_mins=0)
    # With 0 timeout, session expires immediately
    assert session.is_expired()


def test_session_manager_create():
    mgr = SessionManager(timeout_mins=30)
    session = mgr.create()
    assert session.id is not None
    assert len(session.id) == 32  # uuid hex


def test_session_manager_get():
    mgr = SessionManager(timeout_mins=30)
    session = mgr.create()
    retrieved = mgr.get(session.id)
    assert retrieved is not None
    assert retrieved.id == session.id


def test_session_manager_get_nonexistent():
    mgr = SessionManager(timeout_mins=30)
    assert mgr.get("nonexistent") is None


def test_session_manager_delete():
    mgr = SessionManager(timeout_mins=30)
    session = mgr.create()
    assert mgr.delete(session.id) is True
    assert mgr.get(session.id) is None


def test_session_manager_delete_nonexistent():
    mgr = SessionManager(timeout_mins=30)
    assert mgr.delete("nonexistent") is False


def test_session_manager_cleanup_expired():
    mgr = SessionManager(timeout_mins=0)  # Immediate expiry
    session = mgr.create()
    session_id = session.id
    mgr.cleanup_expired()
    assert mgr.get(session_id) is None


def test_session_uses_utc():
    session = Session("test", timeout_mins=30)
    assert session.created_at.tzinfo == timezone.utc
    assert session.last_accessed.tzinfo == timezone.utc
    session.touch()
    assert session.last_accessed.tzinfo == timezone.utc


def test_create_triggers_cleanup():
    """Creating a session cleans up expired ones."""
    mgr = SessionManager(timeout_mins=0)  # Immediate expiry
    s1 = mgr.create()
    s1_id = s1.id

    # Verify s1 is still in the internal dict (not yet cleaned up)
    assert s1_id in mgr._sessions

    # s1 is now expired. Creating s2 should clean it up.
    s2 = mgr.create()
    assert s2.id != s1_id

    # Verify s1 is actually gone from internal dict (not just lazily expired on get)
    assert s1_id not in mgr._sessions


def test_session_manager_with_seed_data():
    """Sessions should be seeded with base tables when seed_data is provided."""
    seed = [
        ("products", pl.DataFrame({"id": [1, 2], "name": ["A", "B"]})),
        ("sales", pl.DataFrame({"id": [1], "amount": [100.0]})),
    ]
    mgr = SessionManager(timeout_mins=30, seed_data=seed)
    session = mgr.create()

    # Tables should be registered
    assert "products" in session.tables
    assert "sales" in session.tables

    # Data should be queryable
    result = session.duckdb.execute_sql("SELECT count(*) AS n FROM products")
    assert result["n"][0] == 2


def test_session_manager_without_seed_data():
    """Sessions without seed_data should start empty (backward compatible)."""
    mgr = SessionManager(timeout_mins=30)
    session = mgr.create()
    assert session.tables == []


def test_session_seed_data_isolated_between_sessions():
    """Each session should get its own copy of seed data."""
    seed = [("t", pl.DataFrame({"x": [1]}))]
    mgr = SessionManager(timeout_mins=30, seed_data=seed)
    s1 = mgr.create()
    s2 = mgr.create()

    # Both have the table
    assert "t" in s1.tables
    assert "t" in s2.tables

    # They should be independent DuckDB instances
    assert s1.duckdb is not s2.duckdb


def test_load_seed_data_csv(tmp_path):
    """load_seed_data should parse CSV files."""
    from ggsql_rest._sessions import load_seed_data

    csv_file = tmp_path / "sales.csv"
    csv_file.write_text("id,amount\n1,100\n2,200\n")

    result = load_seed_data([str(csv_file)])
    assert len(result) == 1
    assert result[0][0] == "sales"
    assert len(result[0][1]) == 2


def test_load_seed_data_missing_file():
    """load_seed_data should raise for missing files."""
    from ggsql_rest._sessions import load_seed_data
    import pytest

    with pytest.raises(FileNotFoundError):
        load_seed_data(["/nonexistent/file.csv"])


def test_make_sample_data():
    """make_sample_data should return products, sales, and employees tables."""
    from ggsql_rest._sessions import make_sample_data

    seed = make_sample_data()
    names = [name for name, _ in seed]
    assert names == ["products", "sales", "employees"]

    # Verify row counts match Rust server
    by_name = {name: df for name, df in seed}
    assert len(by_name["products"]) == 7
    assert len(by_name["sales"]) == 36
    assert len(by_name["employees"]) == 6

    # Verify column structure
    assert set(by_name["products"].columns) == {"product_id", "product_name", "category", "price"}
    assert set(by_name["sales"].columns) == {"sale_id", "product_id", "quantity", "sale_date", "region"}
    assert set(by_name["employees"].columns) == {"employee_id", "employee_name", "department", "salary", "hire_date"}


def test_make_sample_data_queryable():
    """Sample data should be queryable after seeding a session."""
    from ggsql_rest._sessions import make_sample_data

    seed = make_sample_data()
    mgr = SessionManager(timeout_mins=30, seed_data=seed)
    session = mgr.create()

    result = session.duckdb.execute_sql(
        "SELECT region, SUM(quantity) AS total FROM sales GROUP BY region ORDER BY region"
    )
    assert len(result) == 3  # US, EU, APAC
    assert set(result["region"].to_list()) == {"US", "EU", "APAC"}
