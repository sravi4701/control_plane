"""Uniform error responses: ``{"error": {"code", "message", "details"}}``.

Every failure, including validation errors, unknown routes and unexpected exceptions, goes
through these handlers, so clients can always branch on ``error.code``.
"""

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.schemas import ErrorResponse
from app.domain.errors import (
    DomainError,
    InvalidCursorError,
    TaskNotFoundError,
    TenantAlreadyExistsError,
    TenantNotFoundError,
    TenantUpdateNotAllowedError,
    TenantVersionConflictError,
)

log = logging.getLogger(__name__)

# The single source of truth for error code -> HTTP status.
DOMAIN_ERROR_STATUS: dict[type[DomainError], int] = {
    TenantNotFoundError: 404,
    TaskNotFoundError: 404,
    TenantAlreadyExistsError: 409,
    TenantUpdateNotAllowedError: 409,
    TenantVersionConflictError: 409,
    InvalidCursorError: 422,
}

_HTTP_STATUS_CODES = {404: "not_found", 405: "method_not_allowed"}


def error_response(
    status_code: int, code: str, message: str, details: dict[str, Any] | None = None
) -> JSONResponse:
    body = {"error": {"code": code, "message": message, "details": details or {}}}
    return JSONResponse(status_code=status_code, content=body)


def error_responses(*status_codes: int) -> dict[int | str, dict[str, Any]]:
    """OpenAPI ``responses=`` helper so the error shape is documented on every route."""
    return {code: {"model": ErrorResponse} for code in status_codes}


async def _domain_error(_: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, DomainError):  # registered for DomainError only
        raise exc
    status = DOMAIN_ERROR_STATUS.get(type(exc), 400)
    return error_response(status, exc.code, exc.message, exc.details)


async def _validation_error(_: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, RequestValidationError):  # registered for RequestValidationError only
        raise exc
    errors = [
        {"loc": list(e.get("loc", ())), "msg": e.get("msg", ""), "type": e.get("type", "")}
        for e in exc.errors()
    ]
    return error_response(422, "validation_error", "request validation failed", {"errors": errors})


async def _http_error(_: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, StarletteHTTPException):  # registered for StarletteHTTPException only
        raise exc
    code = _HTTP_STATUS_CODES.get(exc.status_code, "http_error")
    return error_response(exc.status_code, code, str(exc.detail))


async def _unhandled_error(_: Request, exc: Exception) -> JSONResponse:
    log.exception("unhandled error", exc_info=exc)
    return error_response(500, "internal_error", "an unexpected error occurred")


def install_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(DomainError, _domain_error)
    app.add_exception_handler(RequestValidationError, _validation_error)
    app.add_exception_handler(StarletteHTTPException, _http_error)
    app.add_exception_handler(Exception, _unhandled_error)
