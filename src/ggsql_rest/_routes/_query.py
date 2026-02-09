"""Query execution routes."""

from fastapi import APIRouter, Depends, Request

from .._models import (
    QueryRequest,
    QueryResponse,
    QueryMetadata,
    SqlRequest,
    SqlResponse,
    success_envelope,
)
from .._connections import ConnectionRegistry
from .._sessions import Session
from .._query import execute_ggsql, execute_sql
from ._sessions import get_session

router = APIRouter(prefix="/sessions/{session_id}", tags=["query"])


def get_registry() -> ConnectionRegistry:
    """Dependency placeholder — overridden by app factory."""
    raise RuntimeError("ConnectionRegistry not initialized")


@router.post("/query")
def query(
    request: Request,
    body: QueryRequest,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
) -> dict:
    """Execute a ggsql query."""
    engine = None
    if body.connection:
        engine = registry.get_engine(body.connection, request)

    result = execute_ggsql(body.query, session, engine)

    return success_envelope(
        QueryResponse(
            spec=result["spec"],
            metadata=QueryMetadata(**result["metadata"]),
        )
    )


@router.post("/sql")
def sql(
    request: Request,
    body: SqlRequest,
    session: Session = Depends(get_session),
    registry: ConnectionRegistry = Depends(get_registry),
) -> dict:
    """Execute a pure SQL query."""
    engine = None
    if body.connection:
        engine = registry.get_engine(body.connection, request)

    result = execute_sql(body.query, session, engine)

    return success_envelope(SqlResponse(**result))
