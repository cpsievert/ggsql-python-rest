from ggsql import DuckDBReader
from sqlalchemy import Engine, inspect as sa_inspect, text

from ._models import ColumnSchema, TableSchema

_NUMERIC_PREFIXES = (
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "TINYINT",
    "HUGEINT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL",
    "REAL",
    "NUMERIC",
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

    return TableSchema(table_name=table_name, source=None, columns=columns)


def _get_duckdb_column_stats(
    duckdb: DuckDBReader,
    table_name: str,
    col_name: str,
    col_type: str,
) -> dict:
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
    inspector = sa_inspect(engine)
    return inspector.get_table_names()


def get_remote_single_table_schema(
    engine: Engine,
    source_name: str,
    table_name: str,
    include_stats: bool,
) -> TableSchema | None:
    inspector = sa_inspect(engine)
    if not inspector.has_table(table_name):
        return None

    col_infos = inspector.get_columns(table_name)
    batch_stats: dict[str, dict] = {}
    if include_stats:
        col_pairs = [(ci["name"], str(ci["type"])) for ci in col_infos]
        batch_stats = _get_remote_table_stats_batched(engine, table_name, col_pairs)

    columns = []
    for col_info in col_infos:
        col_name = col_info["name"]
        col_type = str(col_info["type"])
        stats = batch_stats.get(col_name, {})
        columns.append(
            ColumnSchema(
                column_name=col_name,
                data_type=col_type,
                **stats,
            )
        )

    return TableSchema(
        table_name=table_name,
        source=source_name,
        columns=columns,
    )


def get_remote_table_schemas(
    engine: Engine,
    source_name: str,
    include_stats: bool,
) -> list[TableSchema]:
    inspector = sa_inspect(engine)
    tables: list[TableSchema] = []

    for table_name in inspector.get_table_names():
        col_infos = inspector.get_columns(table_name)
        batch_stats: dict[str, dict] = {}
        if include_stats:
            col_pairs = [(ci["name"], str(ci["type"])) for ci in col_infos]
            batch_stats = _get_remote_table_stats_batched(engine, table_name, col_pairs)

        columns = []
        for col_info in col_infos:
            col_name = col_info["name"]
            col_type = str(col_info["type"])
            stats = batch_stats.get(col_name, {})
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
                source=source_name,
                columns=columns,
            )
        )

    return tables


def _is_remote_numeric_type(type_str: str) -> bool:
    upper = type_str.upper()
    return any(
        kw in upper for kw in ("INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL")
    )


def _is_remote_text_type(type_str: str) -> bool:
    upper = type_str.upper()
    return any(kw in upper for kw in ("VARCHAR", "TEXT", "CHAR", "STRING"))


def _get_remote_table_stats_batched(
    engine: Engine,
    table_name: str,
    columns: list[tuple[str, str]],
) -> dict[str, dict]:
    """Numeric columns are batched into a single MIN/MAX query.
    Text columns still need individual DISTINCT queries.
    """
    stats: dict[str, dict] = {}

    numeric_cols = [
        (name, typ) for name, typ in columns if _is_remote_numeric_type(typ)
    ]
    if numeric_cols:
        parts = []
        for col_name, _ in numeric_cols:
            parts.append(f'MIN("{col_name}") AS "min_{col_name}"')
            parts.append(f'MAX("{col_name}") AS "max_{col_name}"')
        sql = f'SELECT {", ".join(parts)} FROM "{table_name}"'

        with engine.connect() as conn:
            result = conn.execute(text(sql))
            row = result.fetchone()
            if row:
                for col_name, _ in numeric_cols:
                    col_stats: dict = {}
                    min_val = row._mapping[f"min_{col_name}"]
                    max_val = row._mapping[f"max_{col_name}"]
                    if min_val is not None:
                        col_stats["min_value"] = str(min_val)
                    if max_val is not None:
                        col_stats["max_value"] = str(max_val)
                    stats[col_name] = col_stats

    text_cols = [(name, typ) for name, typ in columns if _is_remote_text_type(typ)]
    for col_name, _ in text_cols:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    f'SELECT DISTINCT "{col_name}" FROM "{table_name}" '
                    f'WHERE "{col_name}" IS NOT NULL LIMIT 21'
                )
            )
            values = [row[0] for row in result.fetchall()]
            if len(values) <= 20:
                stats[col_name] = {"categorical_values": sorted(str(v) for v in values)}

    return stats
