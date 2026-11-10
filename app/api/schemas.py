"""HTTP request and response models."""

import uuid
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from app.db.models import Task, Tenant
from app.domain.state_machine import TaskStatus, TaskType, TenantStatus
from app.timeutil import isoformat_ms

# 3-28 chars, Kubernetes-namespace compatible: lowercase letter first, letter or digit last.
SLUG_PATTERN = r"^[a-z][-a-z0-9]{1,26}[a-z0-9]$"

Slug = Annotated[str, Field(pattern=SLUG_PATTERN, examples=["acme-corp"])]
Name = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]


class TenantCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    slug: Slug
    name: Name


class TenantUpdate(BaseModel):
    """Partial update. ``version`` is required: it is the optimistic-locking precondition."""

    model_config = ConfigDict(extra="forbid")

    name: Name | None = None
    version: Annotated[int, Field(ge=1, description="The tenant version the client last read")]

    @model_validator(mode="after")
    def _at_least_one_field(self) -> "TenantUpdate":
        if self.name is None:
            raise ValueError("at least one mutable field (name) must be provided")
        return self


class TenantOut(BaseModel):
    id: uuid.UUID
    slug: str
    name: str
    status: TenantStatus
    version: int
    created_at: str
    updated_at: str

    @classmethod
    def from_model(cls, t: Tenant) -> "TenantOut":
        return cls(
            id=t.id,
            slug=t.slug,
            name=t.name,
            status=TenantStatus(t.status),
            version=t.version,
            created_at=isoformat_ms(t.created_at),
            updated_at=isoformat_ms(t.updated_at),
        )


class TaskOut(BaseModel):
    id: uuid.UUID
    type: TaskType
    tenant_id: uuid.UUID
    status: TaskStatus
    error: str | None
    created_at: str
    updated_at: str

    @classmethod
    def from_model(cls, t: Task) -> "TaskOut":
        return cls(
            id=t.id,
            type=TaskType(t.type),
            tenant_id=t.tenant_id,
            status=TaskStatus(t.status),
            error=t.error,
            created_at=isoformat_ms(t.created_at),
            updated_at=isoformat_ms(t.updated_at),
        )


class TenantWithTask(BaseModel):
    """Returned by every mutating tenant call: the new tenant state plus the task it spawned."""

    tenant: TenantOut
    task: TaskOut


class TenantPage(BaseModel):
    items: list[TenantOut]
    next_cursor: str | None


class TaskPage(BaseModel):
    items: list[TaskOut]
    next_cursor: str | None


class ErrorBody(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    error: ErrorBody
