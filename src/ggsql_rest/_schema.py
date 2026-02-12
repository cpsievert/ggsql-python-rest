"""Schema extraction for local DuckDB and remote SQLAlchemy databases."""

from ggsql import DuckDBReader
from sqlalchemy import Engine, inspect as sa_inspect, text

from ._models import ColumnSchema, TableSchema

# DuckDB type classification
_NUMERIC_PREFIXES = (
    "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT",
    "FLOAT", "DOUBLE", "DECIMAL", "REAL", "NUMERIC",
)
_TEXT_PREFIXES = ("VARCHAR", "TEXT", "STRING", "CHAR")


def _is_numeric_type(data_type: str) -> bool:
    upper = data_type.upper()
    return any(upper.startswith(prefix) for prefix in _NUMERIC_PREFIXES)


def _is_text_type(data_type: str) -> bool:
    upper = data_type.upper()
    return any(upper.startswith(prefix) for prefix in _TEXT_PREFIXES)


def get_local_table_schema(
    duckdb: DuckDBReader,
    table_name: str,
    include_stats: bool,
) -> TableSchema:
    """Extract schema for a local DuckDB table."""
    describe_df = duckdb.execute_sql(f'DESCRIBE "{table_name}"')
    columns: list[ColumnSchema] = []

    for row in describe_df.iter_rows(named=True):
        col_name = row["column_name"]
        col_type = row["column_type"]

        stats: dict = {}
        if include_stats:
            stats = _get_duckdb_column_stats(duckdb, table_name, col_name, col_type)

        columns.append(
            ColumnSchema(
                column_name=col_name,
                data_type=col_type,
                **stats,
            )
        )

    return TableSchema(table_name=table_name, connection=None, columns=columns)


def _get_duckdb_column_stats(
    duckdb: DuckDBReader,
    table_name: str,
    col_name: str,
    col_type: str,
) -> dict:
    """Get column statistics from DuckDB."""
    stats: dict = {}

    if _is_numeric_type(col_type):
        result = duckdb.execute_sql(
            f'SELECT MIN("{col_name}") AS min_val, MAX("{col_name}") AS max_val FROM "{table_name}"'
        )
        row = result.row(0, named=True)
        if row["min_val"] is not None:
            stats["min_value"] = str(row["min_val"])
        if row["max_val"] is not None:
            stats["max_value"] = str(row["max_val"])

    elif _is_text_type(col_type):
        result = duckdb.execute_sql(
            f'SELECT DISTINCT "{col_name}" FROM "{table_name}" WHERE "{col_name}" IS NOT NULL LIMIT 21'
        )
        values = result[col_name].to_list()
        if len(values) <= 20:
            stats["categorical_values"] = sorted(str(v) for v in values)

    return stats


def get_remote_table_names(engine: Engine) -> list[str]:
    """Get table names from a remote database (no column introspection)."""
    inspector = sa_inspect(engine)
    return inspector.get_table_names()


def get_remote_table_schemas(
    engine: Engine,
    connection_name: str,
    include_stats: bool,
) -> list[TableSchema]:
    """Extract schema for all tables in a remote database."""
    inspector = sa_inspect(engine)
    tables: list[TableSchema] = []

    for table_name in inspector.get_table_names():
        columns: list[ColumnSchema] = []

        for col_info in inspector.get_columns(table_name):
            col_name = col_info["name"]
            col_type = str(col_info["type"])

            stats: dict = {}
            if include_stats:
                stats = _get_remote_column_stats(
                    engine, table_name, col_name, col_type
                )

            columns.append(
                ColumnSchema(
                    column_name=col_name,
                    data_type=col_type,
                    **stats,
                )
            )

        tables.append(
            TableSchema(
                table_name=table_name,
                connection=connection_name,
                columns=columns,
            )
        )

    return tables


def _is_remote_numeric_type(type_str: str) -> bool:
    """Check if a SQLAlchemy type string represents a numeric type."""
    upper = type_str.upper()
    return any(kw in upper for kw in ("INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL"))


def _is_remote_text_type(type_str: str) -> bool:
    """Check if a SQLAlchemy type string represents a text type."""
    upper = type_str.upper()
    return any(kw in upper for kw in ("VARCHAR", "TEXT", "CHAR", "STRING"))


def _get_remote_column_stats(
    engine: Engine,
    table_name: str,
    col_name: str,
    col_type: str,
) -> dict:
    """Get column statistics from a remote database."""
    stats: dict = {}

    with engine.connect() as conn:
        if _is_remote_numeric_type(col_type):
            result = conn.execute(
                text(f'SELECT MIN("{col_name}"), MAX("{col_name}") FROM "{table_name}"')
            )
            row = result.fetchone()
            if row and row[0] is not None:
                stats["min_value"] = str(row[0])
            if row and row[1] is not None:
                stats["max_value"] = str(row[1])

        elif _is_remote_text_type(col_type):
            result = conn.execute(
                text(
                    f'SELECT DISTINCT "{col_name}" FROM "{table_name}" '
                    f'WHERE "{col_name}" IS NOT NULL LIMIT 21'
                )
            )
            values = [row[0] for row in result.fetchall()]
            if len(values) <= 20:
                stats["categorical_values"] = sorted(str(v) for v in values)

    return stats
