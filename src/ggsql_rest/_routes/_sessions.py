import io
import re
from pathlib import Path

from fastapi import APIRouter, Depends, Form, UploadFile
import polars as pl

from .._errors import invalid_request, session_not_found
from .._models import SessionResponse, TablesResponse, UploadResponse, success_envelope
from .._sessions import Session, SessionManager

router = APIRouter(prefix="/sessions", tags=["sessions"])


def get_session_manager() -> SessionManager:
    """Placeholder — overridden by app factory."""
    raise RuntimeError("SessionManager not initialized")


def get_session(
    session_id: str,
    session_mgr: SessionManager = Depends(get_session_manager),
) -> Session:
    session = session_mgr.get(session_id)
    if session is None:
        raise session_not_found(session_id)
    return session


@router.post("")
async def create_session(
    session_mgr: SessionManager = Depends(get_session_manager),
) -> dict:
    session = session_mgr.create()
    return success_envelope(SessionResponse(session_id=session.id))


@router.delete("/{session_id}")
async def delete_session(
    session_id: str,
    session_mgr: SessionManager = Depends(get_session_manager),
) -> dict:
    if not session_mgr.delete(session_id):
        raise session_not_found(session_id)
    return success_envelope()


@router.get("/{session_id}/tables")
def list_tables(session: Session = Depends(get_session)) -> dict:
    return success_envelope(TablesResponse(tables=session.tables))


def sanitize_table_name(stem: str, existing_tables: list[str]) -> str:
    name = re.sub(r"[^a-zA-Z0-9_]", "_", stem)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    name = name or "unnamed"

    base_name = name
    counter = 2
    while name in existing_tables:
        name = f"{base_name}_{counter}"
        counter += 1

    return name


@router.post("/{session_id}/upload")
async def upload_file(
    file: UploadFile,
    table_name: str | None = Form(None),
    session: Session = Depends(get_session),
) -> dict:
    if file.filename is None:
        raise invalid_request("Filename is required")

    if table_name is None:
        table_name = sanitize_table_name(Path(file.filename).stem, session.tables)

    content = await file.read()
    extension = Path(file.filename).suffix.lower()

    if extension == ".csv":
        df = pl.read_csv(io.BytesIO(content), null_values=["NA"])
    elif extension == ".parquet":
        df = pl.read_parquet(io.BytesIO(content))
    elif extension in (".json", ".jsonl", ".ndjson"):
        df = pl.read_json(io.BytesIO(content))
    else:
        raise invalid_request(f"Unsupported file format: {extension}")

    session.duckdb.register(table_name, df)
    session.tables.append(table_name)

    return success_envelope(
        UploadResponse(
            table_name=table_name,
            row_count=len(df),
            columns=df.columns,
        )
    )
