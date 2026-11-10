import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import Base, OutboxEvent, Task, Tenant
from app.messaging.schemas import TaskProgressUpdate
from app.services.progress_service import ApplyOutcome, apply_progress_update
from app.timeutil import utcnow


def progress(
    task_id: uuid.UUID | str, status: str, *, message_id: uuid.UUID | None = None, **extra: Any
) -> TaskProgressUpdate:
    return TaskProgressUpdate.model_validate(
        {
            "message_id": message_id or uuid.uuid4(),
            "task_id": task_id,
            "status": status,
            "occurred_at": utcnow(),
            **extra,
        }
    )


async def apply(sm: async_sessionmaker[AsyncSession], update: TaskProgressUpdate) -> ApplyOutcome:
    async with sm() as session:
        return await apply_progress_update(session, update)


async def finish_task(
    sm: async_sessionmaker[AsyncSession], task_id: uuid.UUID | str, status: str = "done"
) -> None:
    """Drive a task to a terminal state the way the worker would."""
    await apply(sm, progress(task_id, "in_progress"))
    await apply(sm, progress(task_id, status))


async def get_tenant(sm: async_sessionmaker[AsyncSession], tenant_id: uuid.UUID | str) -> Tenant:
    async with sm() as session:
        tenant = await session.get(Tenant, uuid.UUID(str(tenant_id)))
        assert tenant is not None
        return tenant


async def get_task(sm: async_sessionmaker[AsyncSession], task_id: uuid.UUID | str) -> Task:
    async with sm() as session:
        task = await session.get(Task, uuid.UUID(str(task_id)))
        assert task is not None
        return task


async def count(sm: async_sessionmaker[AsyncSession], model: type[Base]) -> int:
    async with sm() as session:
        return int(await session.scalar(select(func.count()).select_from(model)) or 0)


async def outbox_rows(sm: async_sessionmaker[AsyncSession]) -> list[OutboxEvent]:
    async with sm() as session:
        return list((await session.scalars(select(OutboxEvent).order_by(OutboxEvent.id))).all())
