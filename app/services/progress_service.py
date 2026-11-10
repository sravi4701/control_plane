"""Applying worker progress updates: idempotent and order-tolerant.

One transaction per update:
1. Lock the task row (``SELECT ... FOR UPDATE``) so every update for one task is serialized,
   including two copies of the same message being processed at the same moment.
2. Record ``message_id`` in ``processed_messages`` with ``ON CONFLICT DO NOTHING``. If the row
   already exists, this is a duplicate: change nothing and acknowledge it.
3. Apply only strictly forward transitions. Stale updates (or any update for a terminal task)
   are recorded and acknowledged without changing state.
4. On a terminal outcome, move the tenant with a conditional UPDATE (and bump its version).
"""

import logging
from enum import StrEnum

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import ProcessedMessage, Task, Tenant
from app.domain.state_machine import (
    IN_FLIGHT_STATUS,
    TaskStatus,
    TaskType,
    can_advance_task,
    tenant_status_after_task,
)
from app.messaging.schemas import TaskProgressUpdate
from app.timeutil import utcnow

log = logging.getLogger(__name__)


class ApplyOutcome(StrEnum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"
    STALE = "stale"


class UnknownTaskError(Exception):
    """The update refers to a task that does not exist: a poison message.

    Tasks are published only after they are committed, so a legitimate update can never arrive
    before its task exists.
    """


async def apply_progress_update(session: AsyncSession, msg: TaskProgressUpdate) -> ApplyOutcome:
    async with session.begin():
        now = utcnow()
        task = (
            await session.scalars(select(Task).where(Task.id == msg.task_id).with_for_update())
        ).one_or_none()
        if task is None:
            raise UnknownTaskError(f"task '{msg.task_id}' does not exist")

        new_status = msg.task_status
        current = TaskStatus(task.status)
        outcome = (
            ApplyOutcome.APPLIED if can_advance_task(current, new_status) else ApplyOutcome.STALE
        )

        recorded = await session.scalar(
            insert(ProcessedMessage)
            .values(
                message_id=msg.message_id,
                task_id=msg.task_id,
                outcome=outcome.value,
                processed_at=now,
            )
            .on_conflict_do_nothing(index_elements=[ProcessedMessage.message_id])
            .returning(ProcessedMessage.message_id)
        )
        if recorded is None:
            log.info("duplicate update %s for task %s ignored", msg.message_id, msg.task_id)
            return ApplyOutcome.DUPLICATE

        if outcome is ApplyOutcome.STALE:
            log.info(
                "stale update %s for task %s ignored (%s -> %s)",
                msg.message_id,
                msg.task_id,
                current,
                new_status,
            )
            return outcome

        task.status = new_status.value
        task.updated_at = now
        if new_status == TaskStatus.FAILED:
            task.error = msg.error

        tenant_status = tenant_status_after_task(TaskType(task.type), new_status)
        if tenant_status is not None:
            await _move_tenant(session, task, tenant_status.value)
    return outcome


async def _move_tenant(session: AsyncSession, task: Task, new_status: str) -> None:
    expected = IN_FLIGHT_STATUS[TaskType(task.type)].value
    result = await session.execute(
        update(Tenant)
        .where(Tenant.id == task.tenant_id, Tenant.status == expected)
        .values(status=new_status, version=Tenant.version + 1, updated_at=utcnow())
        .returning(Tenant.id)
    )
    if result.scalar_one_or_none() is None:
        # Unreachable while the invariants hold (one open task per tenant, and the tenant stays
        # in its in-flight status until that task ends). Log loudly rather than corrupt state.
        log.error(
            "tenant %s not in expected status %r for terminal %s task %s; tenant left unchanged",
            task.tenant_id,
            expected,
            task.type,
            task.id,
        )
