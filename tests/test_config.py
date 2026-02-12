"""Tests for YAML connection config loading."""

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from ggsql_rest._config import load_connections_from_yaml


def test_load_basic_connections(tmp_path: Path):
    config_file = tmp_path / "connections.yaml"
    config_file.write_text(textwrap.dedent("""\
        connections:
          test_db:
            url: "sqlite:///:memory:"
    """))

    registry = load_connections_from_yaml(config_file)
    assert "test_db" in registry.list_connections()


def test_load_with_engine_kwargs(tmp_path: Path):
    config_file = tmp_path / "connections.yaml"
    config_file.write_text(textwrap.dedent("""\
        connections:
          test_db:
            url: "sqlite:///:memory:"
            echo: true
            pool_pre_ping: true
    """))

    registry = load_connections_from_yaml(config_file)
    assert "test_db" in registry.list_connections()


def test_load_multiple_connections(tmp_path: Path):
    config_file = tmp_path / "connections.yaml"
    config_file.write_text(textwrap.dedent("""\
        connections:
          db1:
            url: "sqlite:///:memory:"
          db2:
            url: "sqlite:///:memory:"
    """))

    registry = load_connections_from_yaml(config_file)
    assert set(registry.list_connections()) == {"db1", "db2"}


def test_load_missing_connections_key(tmp_path: Path):
    config_file = tmp_path / "connections.yaml"
    config_file.write_text("something_else: true\n")

    with pytest.raises(ValueError, match="connections"):
        load_connections_from_yaml(config_file)


def test_load_missing_url(tmp_path: Path):
    config_file = tmp_path / "connections.yaml"
    config_file.write_text(textwrap.dedent("""\
        connections:
          bad_db:
            echo: true
    """))

    with pytest.raises(ValueError, match="url"):
        load_connections_from_yaml(config_file)


def test_load_empty_connections(tmp_path: Path):
    config_file = tmp_path / "connections.yaml"
    config_file.write_text("connections: {}\n")

    registry = load_connections_from_yaml(config_file)
    assert registry.list_connections() == []


def test_cli_help():
    """CLI entry point should respond to --help."""
    result = subprocess.run(
        [sys.executable, "-m", "ggsql_rest", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "--connections" in result.stdout
    assert "--port" in result.stdout
    assert "--host" in result.stdout
    assert "--load-data" in result.stdout
    assert "--load-sample-data" in result.stdout


def test_load_connections_derives_provider_from_url(tmp_path: Path):
    """Provider is derived from SQLAlchemy URL scheme."""
    config_file = tmp_path / "connections.yaml"
    config_file.write_text(textwrap.dedent("""\
        connections:
          pg_db:
            url: "postgresql+psycopg2://user:pass@localhost/db"
          mysql_db:
            url: "mysql://user:pass@localhost/db"
          sqlite_db:
            url: "sqlite:///path/to/db.sqlite"
    """))

    registry = load_connections_from_yaml(config_file)
    assert registry.get_provider("pg_db") == "postgresql"
    assert registry.get_provider("mysql_db") == "mysql"
    assert registry.get_provider("sqlite_db") == "sqlite"


def test_load_connections_handles_url_without_scheme(tmp_path: Path):
    """Malformed URLs without scheme don't crash."""
    config_file = tmp_path / "connections.yaml"
    config_file.write_text(textwrap.dedent("""\
        connections:
          weird_db:
            url: "not-a-real-url"
    """))

    registry = load_connections_from_yaml(config_file)
    assert registry.get_provider("weird_db") is None
