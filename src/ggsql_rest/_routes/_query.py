from typing import Any

from fastapi import APIRouter, Depends, Request
from sqlalchemy import Engine
from ggsql import validate

from .._models import (
    QueryRequest,
    QueryResponse,
    QueryMetadata,
    SqlRequest,
    SqlResponse,
    ValidateRequest,
    ValidateResponse,
    ValidationResult,
    success_envelope,
)
from .._connections import ConnectionRegistry
from .._sessions import Session
from .._snowflake import SnowflakeDiscovery
from .._pins import PinsDiscovery
from .._query import execute_ggsql, execute_sql
from ._sessions import get_session
from ._dependencies import get_registry, get_snowflake_discovery, get_pins_discovery

router = APIRouter(prefix="/sessions/{session_id}", tags=["query"])


def resolve_source(
    source: str,
    request: Request,
    registry: ConnectionRegistry,
    snowflake: SnowflakeDiscovery | None,
) -> tuple[Engine, Any | None]:
    """Resolve a source name to (engine, optional_adbc_conn)."""
    if source in registry.list_connections():
        return registry.get_engine(source, request), None
    if snowflake is not None and snowflake.has_connection(source, request):
        engine = snowflake.get_engine(source, request)
        adbc_conn = snowflake.get_adbc_connection(source, request)
        return engine, adbc_conn
    raise KeyError(f"Unknown source: '{source}'")


@router.post("/query")
async def query(
    request: Request,
    body: QueryRequest,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
    snowflake: SnowflakeDiscovery | None = Depends(get_snowflake_discovery),
    pins: PinsDiscovery | None = Depends(get_pins_discovery),
) -> dict:
    engine = None
    adbc_conn = None
    if body.source:
        if body.provider == "pins" and pins is not None:
            pins.load_pins_for_query(body.query, request, session)
        else:
            engine, adbc_conn = resolve_source(
                body.source, request, registry, snowflake
            )

    result = execute_ggsql(
        body.query, session, engine, max_rows=body.max_rows, adbc_conn=adbc_conn
    )

    return success_envelope(
        QueryResponse(
            spec=result["spec"],
            metadata=QueryMetadata(**result["metadata"]),
        )
    )


@router.post("/sql")
async def sql(
    request: Request,
    body: SqlRequest,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
    snowflake: SnowflakeDiscovery | None = Depends(get_snowflake_discovery),
    pins: PinsDiscovery | None = Depends(get_pins_discovery),
) -> dict:
    engine = None
    adbc_conn = None
    if body.source:
        if body.provider == "pins" and pins is not None:
            pins.load_pins_for_query(body.query, request, session)
        else:
            engine, adbc_conn = resolve_source(
                body.source, request, registry, snowflake
            )

    result = execute_sql(
        body.query,
        session,
        engine,
        timeout_seconds=body.timeout_seconds,
        adbc_conn=adbc_conn,
    )

    return success_envelope(SqlResponse(**result))


@router.post("/validate")
async def validate_queries(
    body: ValidateRequest,
    session: Session = Depends(get_session),
) -> dict:
    """Validate a batch of ggsql queries.

    For each query:
    1. Parse with ggsql.validate() to catch parse errors
    2. If the query has VISUALISE, extract SQL with .sql()
    3. If no VISUALISE, treat the whole query as SQL
    4. Run EXPLAIN on the SQL to catch column/table errors
    5. Return {valid: true} or {valid: false, error: "..."}

    All exceptions are caught per-query so one bad query doesn't abort the batch.
    """
    results: list[ValidationResult] = []

    for item in body.queries:
        try:
            # Step 1: Parse with ggsql.validate
            validated = validate(item.query)

            # Check for parse errors
            parse_errors = [
                e for e in validated.errors()
                if e.get("message", "").startswith("Parse error")
            ]
            if parse_errors:
                messages = "; ".join(e["message"] for e in parse_errors)
                results.append(ValidationResult(valid=False, error=messages))
                continue

            # Step 2: Determine SQL to validate
            if validated.has_visual():
                # Query has VISUALISE — extract SQL portion
                sql = validated.sql()
            else:
                # Plain SQL query
                sql = item.query

            # Step 3: Run EXPLAIN to catch semantic errors
            if sql.strip():
                try:
                    session.duckdb.execute_sql(f"EXPLAIN {sql}")
                except Exception as e:
                    # DuckDB error — invalid SQL
                    results.append(ValidationResult(valid=False, error=str(e)))
                    continue

            # Query is valid
            results.append(ValidationResult(valid=True))

        except Exception as e:
            # Catch-all for any unexpected errors
            results.append(ValidationResult(valid=False, error=str(e)))

    return success_envelope(ValidateResponse(results=results))
