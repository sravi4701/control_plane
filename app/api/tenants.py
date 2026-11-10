from typing import Annotated

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session, parse_id
from app.api.errors import error_responses
from app.api.schemas import (
    TaskOut,
    TaskPage,
    TenantCreate,
    TenantOut,
    TenantPage,
    TenantUpdate,
    TenantWithTask,
)
from app.db.models import Task, Tenant
from app.domain.errors import TenantNotFoundError
from app.domain.state_machine import TaskStatus, TaskType, TenantStatus
from app.services import tenant_service as svc

router = APIRouter(prefix="/v1/tenants", tags=["tenants"])

Session = Annotated[AsyncSession, Depends(get_session)]
Limit = Annotated[int, Query(ge=1, le=100, description="Page size")]
CursorParam = Annotated[str | None, Query(description="Opaque cursor from `next_cursor`")]


def _with_task(pair: tuple[Tenant, Task]) -> TenantWithTask:
    tenant, task = pair
    return TenantWithTask(tenant=TenantOut.from_model(tenant), task=TaskOut.from_model(task))


@router.post("", status_code=201, responses=error_responses(409, 422))
async def create_tenant(body: TenantCreate, response: Response, session: Session) -> TenantWithTask:
    """Create a tenant (`provisioning`) and emit its `deploy` task."""
    result = _with_task(await svc.create_tenant(session, body.slug, body.name))
    response.headers["Location"] = f"/v1/tenants/{result.tenant.id}"
    return result


@router.get("", responses=error_responses(422))
async def list_tenants(
    session: Session,
    status: TenantStatus | None = None,
    limit: Limit = 20,
    cursor: CursorParam = None,
) -> TenantPage:
    page = await svc.list_tenants(session, status=status, limit=limit, cursor=cursor)
    return TenantPage(
        items=[TenantOut.from_model(t) for t in page.items], next_cursor=page.next_cursor
    )


@router.get("/{tenant_id}", responses=error_responses(404))
async def get_tenant(tenant_id: str, session: Session) -> TenantOut:
    tid = parse_id(tenant_id, TenantNotFoundError, "tenant_id")
    return TenantOut.from_model(await svc.get_tenant(session, tid))


@router.patch("/{tenant_id}", status_code=202, responses=error_responses(404, 409, 422))
async def update_tenant(tenant_id: str, body: TenantUpdate, session: Session) -> TenantWithTask:
    """Partially update a tenant (only from `active`); it moves to `updating` and an `update`
    task is emitted. `version` must equal the current version (optimistic locking)."""
    tid = parse_id(tenant_id, TenantNotFoundError, "tenant_id")
    return _with_task(
        await svc.update_tenant(session, tid, name=body.name, expected_version=body.version)
    )


@router.delete("/{tenant_id}", status_code=202, responses=error_responses(404, 409))
async def delete_tenant(tenant_id: str, session: Session) -> TenantWithTask:
    """Start destroying a tenant (from `active` or `failed`); a `destroy` task is emitted.
    The record is kept with status `destroyed` so its slug stays reserved."""
    tid = parse_id(tenant_id, TenantNotFoundError, "tenant_id")
    return _with_task(await svc.delete_tenant(session, tid))


@router.get("/{tenant_id}/tasks", responses=error_responses(404, 422))
async def list_tenant_tasks(
    tenant_id: str,
    session: Session,
    type: TaskType | None = None,
    status: TaskStatus | None = None,
    limit: Limit = 20,
    cursor: CursorParam = None,
) -> TaskPage:
    tid = parse_id(tenant_id, TenantNotFoundError, "tenant_id")
    await svc.get_tenant(session, tid)  # 404 for an unknown tenant rather than an empty list
    page = await svc.list_tasks(
        session, tenant_id=tid, task_type=type, status=status, limit=limit, cursor=cursor
    )
    return TaskPage(items=[TaskOut.from_model(t) for t in page.items], next_cursor=page.next_cursor)
