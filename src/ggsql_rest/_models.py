"""Pydantic request/response models."""

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


# === Base class for camelCase serialization ===


class CamelModel(BaseModel):
    """Base model that serializes to camelCase."""

    model_config = ConfigDict(
        populate_by_name=True,
        alias_generator=to_camel,
    )


def success_envelope(data: CamelModel | None = None) -> dict:
    """Wrap response data in success envelope."""
    if data is None:
        return {"status": "success", "data": None}
    return {"status": "success", "data": data.model_dump(by_alias=True)}


# === Requests ===


class QueryRequest(CamelModel):
    """Request body for ggsql query execution."""

    query: str
    connection: str | None = None


class SqlRequest(CamelModel):
    """Request body for pure SQL execution."""

    query: str
    connection: str | None = None


# === Responses ===


class SessionResponse(CamelModel):
    """Response for session creation."""

    session_id: str


class UploadResponse(CamelModel):
    """Response for file upload."""

    table_name: str
    row_count: int
    columns: list[str]


class TablesResponse(CamelModel):
    """Response for listing tables."""

    tables: list[str]


class QueryMetadata(CamelModel):
    """Metadata about query execution."""

    rows: int
    columns: list[str]
    layers: int


class QueryResponse(CamelModel):
    """Response for ggsql query execution."""

    spec: dict
    metadata: QueryMetadata


class SqlResponse(CamelModel):
    """Response for pure SQL execution."""

    rows: list[dict]
    columns: list[str]
    row_count: int
    truncated: bool


class ColumnSchema(CamelModel):
    """Schema for a single column."""

    column_name: str
    data_type: str
    min_value: str | None = None
    max_value: str | None = None
    categorical_values: list[str] | None = None


class TableSchema(CamelModel):
    """Schema for a single table."""

    table_name: str
    connection: str | None = None
    columns: list[ColumnSchema]


class SchemaResponse(CamelModel):
    """Response for schema endpoint."""

    tables: list[TableSchema]


# === Errors ===


class ErrorDetail(BaseModel):
    """Error details."""

    message: str
    type: str


class ErrorResponse(BaseModel):
    """Error response."""

    status: str = "error"
    error: ErrorDetail
