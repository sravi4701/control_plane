"""Domain errors. Each carries a stable, machine-readable ``code`` that is part of the API contract.

The HTTP status for each code lives in one table in ``app.api.errors`` so the mapping is
consistent and documented in one place.
"""

from typing import Any


class DomainError(Exception):
    code: str = "domain_error"

    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(message)
        self.message = message
        self.details = details


class TenantNotFoundError(DomainError):
    code = "tenant_not_found"


class TenantAlreadyExistsError(DomainError):
    code = "tenant_already_exists"


class TenantUpdateNotAllowedError(DomainError):
    code = "tenant_update_not_allowed"


class TenantVersionConflictError(DomainError):
    code = "tenant_version_conflict"


class TaskNotFoundError(DomainError):
    code = "task_not_found"


class InvalidCursorError(DomainError):
    code = "invalid_cursor"
