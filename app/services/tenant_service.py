"""Tenant use cases. Every mutation is a single transaction that writes the tenant, the new task
and its outbox event together; the broker is never touched here.

Guards are expressed as *conditional* UPDATEs (``WHERE status IN (...) AND version = ...``) so the
check and the write are one atomic statement. Two racing requests cannot both pass a guard:
Postgres row locking makes the second UPDATE re-evaluate its WHERE clause against the first
one's committed row, find it no longer matches, and update zero rows.
"""

import uuid
from typing import NoReturn

from sqlalchemy import Select, literal, select, tuple_, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Task, Tenant
from app.domain.errors import (
    TaskNotFoundError,
    TenantAlreadyExistsError,
    TenantNotFoundError,
    TenantUpdateNotAllowedError,
    TenantVersionConflictError,
)
from app.domain.state_machine import (
    DELETABLE_FROM,
    UPDATABLE_FROM,
    TaskStatus,
    TaskType,
    TenantStatus,
)
from app.messaging.outbox import enqueue_task_event
from app.services.pagination import Cursor, Page
from app.timeutil import utcnow

SLUG_UNIQUE_CONSTRAINT = "uq_tenants_slug"


def _new_task(tenant_id: uuid.UUID, task_type: TaskType) -> Task:
    now = utcnow()
    return Task(
        id=uuid.uuid4(),
        type=task_type.value,
        tenant_id=tenant_id,
        status=TaskStatus.ACCEPTED.value,
        error=None,
        created_at=now,
        updated_at=now,
    )


async def _add_task_with_event(session: AsyncSession, tenant_id: uuid.UUID, kind: TaskType) -> Task:
    task = _new_task(tenant_id, kind)
    session.add(task)
    await session.flush()  # task row must exist before the outbox row that references it
    await enqueue_task_event(session, task)
    return task


async def create_tenant(session: AsyncSession, slug: str, name: str) -> tuple[Tenant, Task]:
    now = utcnow()
    tenant = Tenant(
        id=uuid.uuid4(),
        slug=slug,
        name=name,
        status=TenantStatus.PROVISIONING.value,
        version=1,
        created_at=now,
        updated_at=now,
    )
    try:
        async with session.begin():
            session.add(tenant)
            # Flush now so a duplicate slug fails here, on the unique constraint. Concurrent
            # inserts of the same slug block on the index until the first commits; the loser
            # then gets a unique violation. Exactly one wins, with no check-then-insert race.
            await session.flush()
            task = await _add_task_with_event(session, tenant.id, TaskType.DEPLOY)
    except IntegrityError as exc:
        if SLUG_UNIQUE_CONSTRAINT in str(exc.orig):
            raise TenantAlreadyExistsError(
                f"a tenant with slug '{slug}' already exists", slug=slug
            ) from exc
        raise
    return tenant, task


async def update_tenant(
    session: AsyncSession, tenant_id: uuid.UUID, *, name: str | None, expected_version: int
) -> tuple[Tenant, Task]:
    async with session.begin():
        values: dict[str, object] = {
            "status": TenantStatus.UPDATING.value,
            "version": Tenant.version + 1,
            "updated_at": utcnow(),
        }
        if name is not None:
            values["name"] = name
        stmt = (
            update(Tenant)
            .where(
                Tenant.id == tenant_id,
                Tenant.version == expected_version,
                Tenant.status.in_([s.value for s in UPDATABLE_FROM]),
            )
            .values(**values)
            .returning(Tenant)
            .execution_options(populate_existing=True)
        )
        tenant = (await session.scalars(stmt)).one_or_none()
        if tenant is None:
            await _raise_update_rejection(session, tenant_id, expected_version)
        task = await _add_task_with_event(session, tenant.id, TaskType.UPDATE)
    return tenant, task


async def _raise_update_rejection(
    session: AsyncSession, tenant_id: uuid.UUID, expected_version: int
) -> NoReturn:
    """The conditional UPDATE matched nothing; work out why, for a precise error.

    The version is checked before the status: a client holding a stale version should be told
    it is stale (and re-read) rather than being told about a status it has not seen yet.
    """
    current = await session.get(Tenant, tenant_id, populate_existing=True)
    if current is None:
        raise TenantNotFoundError(f"tenant '{tenant_id}' not found", tenant_id=str(tenant_id))
    if current.version != expected_version:
        raise TenantVersionConflictError(
            "tenant was modified concurrently; re-read it and retry with the current version",
            expected_version=expected_version,
            current_version=current.version,
        )
    raise TenantUpdateNotAllowedError(
        f"tenant cannot be updated while '{current.status}'",
        status=current.status,
        allowed_from=sorted(s.value for s in UPDATABLE_FROM),
    )


async def delete_tenant(session: AsyncSession, tenant_id: uuid.UUID) -> tuple[Tenant, Task]:
    async with session.begin():
        stmt = (
            update(Tenant)
            .where(
                Tenant.id == tenant_id,
                Tenant.status.in_([s.value for s in DELETABLE_FROM]),
            )
            .values(
                status=TenantStatus.DESTROYING.value,
                version=Tenant.version + 1,
                updated_at=utcnow(),
            )
            .returning(Tenant)
            .execution_options(populate_existing=True)
        )
        tenant = (await session.scalars(stmt)).one_or_none()
        if tenant is None:
            current = await session.get(Tenant, tenant_id, populate_existing=True)
            if current is None:
                raise TenantNotFoundError(
                    f"tenant '{tenant_id}' not found", tenant_id=str(tenant_id)
                )
            raise TenantUpdateNotAllowedError(
                f"tenant cannot be deleted while '{current.status}'",
                status=current.status,
                allowed_from=sorted(s.value for s in DELETABLE_FROM),
            )
        task = await _add_task_with_event(session, tenant.id, TaskType.DESTROY)
    return tenant, task


async def get_tenant(session: AsyncSession, tenant_id: uuid.UUID) -> Tenant:
    tenant = await session.get(Tenant, tenant_id)
    if tenant is None:
        raise TenantNotFoundError(f"tenant '{tenant_id}' not found", tenant_id=str(tenant_id))
    return tenant


async def get_task(session: AsyncSession, task_id: uuid.UUID) -> Task:
    task = await session.get(Task, task_id)
    if task is None:
        raise TaskNotFoundError(f"task '{task_id}' not found", task_id=str(task_id))
    return task


async def list_tenants(
    session: AsyncSession, *, status: TenantStatus | None, limit: int, cursor: str | None
) -> Page[Tenant]:
    stmt = select(Tenant)
    if status is not None:
        stmt = stmt.where(Tenant.status == status.value)
    return await _paginate(session, stmt, Tenant, limit=limit, cursor=cursor)


async def list_tasks(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID | None,
    task_type: TaskType | None,
    status: TaskStatus | None,
    limit: int,
    cursor: str | None,
) -> Page[Task]:
    stmt = select(Task)
    if tenant_id is not None:
        stmt = stmt.where(Task.tenant_id == tenant_id)
    if task_type is not None:
        stmt = stmt.where(Task.type == task_type.value)
    if status is not None:
        stmt = stmt.where(Task.status == status.value)
    return await _paginate(session, stmt, Task, limit=limit, cursor=cursor)


async def _paginate[M: (Tenant, Task)](
    session: AsyncSession, stmt: Select[tuple[M]], model: type[M], *, limit: int, cursor: str | None
) -> Page[M]:
    if cursor is not None:
        c = Cursor.decode(cursor)
        stmt = stmt.where(
            tuple_(model.created_at, model.id) < tuple_(literal(c.created_at), literal(c.id))
        )
    stmt = stmt.order_by(model.created_at.desc(), model.id.desc()).limit(limit + 1)
    rows = list((await session.scalars(stmt)).all())
    next_cursor = None
    if len(rows) > limit:
        rows = rows[:limit]
        last = rows[-1]
        next_cursor = Cursor(created_at=last.created_at, id=last.id).encode()
    return Page(items=rows, next_cursor=next_cursor)
