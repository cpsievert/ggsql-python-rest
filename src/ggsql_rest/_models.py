from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class CamelModel(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        alias_generator=to_camel,
    )


def success_envelope(data: CamelModel | None = None) -> dict:
    if data is None:
        return {"status": "success", "data": None}
    return {"status": "success", "data": data.model_dump(by_alias=True)}


class QueryRequest(CamelModel):
    query: str
    source: str | None = None
    provider: str | None = None
    max_rows: int | None = None


class SqlRequest(CamelModel):
    query: str
    source: str | None = None
    provider: str | None = None
    timeout_seconds: int | None = None


class SessionResponse(CamelModel):
    session_id: str


class UploadResponse(CamelModel):
    table_name: str
    row_count: int
    columns: list[str]


class TablesResponse(CamelModel):
    tables: list[str]


class QueryMetadata(CamelModel):
    rows: int
    columns: list[str]
    layers: int
    truncated: bool = False


class QueryResponse(CamelModel):
    spec: dict
    metadata: QueryMetadata


class SqlResponse(CamelModel):
    rows: list[dict]
    columns: list[str]
    row_count: int
    truncated: bool


class ColumnSchema(CamelModel):
    column_name: str
    data_type: str
    min_value: str | None = None
    max_value: str | None = None
    categorical_values: list[str] | None = None


class TableSchema(CamelModel):
    table_name: str
    source: str | None = None
    columns: list[ColumnSchema]


class SchemaResponse(CamelModel):
    tables: list[TableSchema]


class TableNameEntry(CamelModel):
    table_name: str
    source: str | None = None
    provider: str | None = None


class TableNamesResponse(CamelModel):
    tables: list[TableNameEntry]


class ProviderInfo(CamelModel):
    name: str
    label: str
    requires_auth: bool


class ErrorDetail(BaseModel):
    message: str
    type: str


class ErrorResponse(BaseModel):
    status: str = "error"
    error: ErrorDetail
