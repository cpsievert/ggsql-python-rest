"""Schema extraction for local DuckDB and remote SQLAlchemy databases."""

from ggsql import DuckDBReader

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
