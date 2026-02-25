from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


class ApiError(Exception):
    def __init__(self, status_code: int, error_type: str, message: str):
        self.status_code = status_code
        self.error_type = error_type
        self.message = message
        super().__init__(message)


def session_not_found(session_id: str) -> ApiError:
    return ApiError(404, "SessionNotFound", f"Session '{session_id}' not found")


def connection_not_found(name: str) -> ApiError:
    return ApiError(400, "ConnectionNotFound", f"Unknown connection: '{name}'")


def invalid_request(message: str) -> ApiError:
    return ApiError(400, "InvalidRequest", message)


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def handle_api_error(request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "status": "error",
                "error": {"type": exc.error_type, "message": exc.message},
            },
        )

    @app.exception_handler(ValueError)
    async def handle_value_error(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "error": {"type": "InvalidQuery", "message": str(exc)},
            },
        )

    @app.exception_handler(KeyError)
    async def handle_key_error(request: Request, exc: KeyError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "error": {"type": "ConnectionNotFound", "message": str(exc)},
            },
        )
