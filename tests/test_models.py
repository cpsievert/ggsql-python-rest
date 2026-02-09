"""Tests for Pydantic models."""

from ggsql_rest._models import (
    QueryRequest,
    QueryMetadata,
    QueryResponse,
    ErrorDetail,
    ErrorResponse,
    SessionResponse,
    UploadResponse,
    SqlResponse,
    success_envelope,
)


def test_query_request_with_connection():
    req = QueryRequest(
        query="SELECT * FROM t VISUALISE x, y DRAW point", connection="warehouse"
    )
    assert req.query == "SELECT * FROM t VISUALISE x, y DRAW point"
    assert req.connection == "warehouse"


def test_query_request_without_connection():
    req = QueryRequest(query="SELECT * FROM t VISUALISE x, y DRAW point")
    assert req.connection is None


def test_query_response():
    resp = QueryResponse(
        spec={"mark": "point"},
        metadata=QueryMetadata(rows=10, columns=["x", "y"], layers=1),
    )
    assert resp.spec == {"mark": "point"}
    assert resp.metadata.rows == 10


def test_error_response():
    resp = ErrorResponse(error=ErrorDetail(message="bad query", type="ParseError"))
    assert resp.status == "error"
    assert resp.error.message == "bad query"


def test_session_response_serializes_to_camelcase():
    """SessionResponse should serialize session_id to sessionId."""
    resp = SessionResponse(session_id="abc123")
    data = resp.model_dump(by_alias=True)
    assert "sessionId" in data
    assert data["sessionId"] == "abc123"
    assert "session_id" not in data


def test_upload_response_serializes_to_camelcase():
    """UploadResponse should serialize snake_case fields to camelCase."""
    resp = UploadResponse(table_name="test", row_count=10, columns=["a", "b"])
    data = resp.model_dump(by_alias=True)
    assert "tableName" in data
    assert "rowCount" in data
    assert data["tableName"] == "test"
    assert data["rowCount"] == 10
    assert "table_name" not in data
    assert "row_count" not in data


def test_sql_response_serializes_to_camelcase():
    """SqlResponse should serialize row_count to rowCount."""
    resp = SqlResponse(rows=[{"x": 1}], columns=["x"], row_count=1, truncated=False)
    data = resp.model_dump(by_alias=True)
    assert "rowCount" in data
    assert data["rowCount"] == 1
    assert "row_count" not in data


def test_success_envelope_with_data():
    """success_envelope should wrap data correctly."""
    resp = SessionResponse(session_id="abc123")
    envelope = success_envelope(resp)
    assert envelope["status"] == "success"
    assert "data" in envelope
    assert envelope["data"]["sessionId"] == "abc123"


def test_success_envelope_without_data():
    """success_envelope should return None data when called without argument."""
    envelope = success_envelope()
    assert envelope == {"status": "success", "data": None}
