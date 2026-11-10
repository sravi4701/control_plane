"""Writing to the transactional outbox. Called inside the same transaction as the task insert."""

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutboxEvent, Task
from app.messaging.topology import task_routing_key
from app.timeutil import isoformat_ms

OUTBOX_NOTIFY_CHANNEL = "outbox_new"


def task_event_payload(task: Task) -> dict[str, Any]:
    return {
        "id": str(task.id),
        "type": task.type,
        "tenant_id": str(task.tenant_id),
        "status": task.status,
        "created_at": isoformat_ms(task.created_at),
    }


async def enqueue_task_event(session: AsyncSession, task: Task) -> None:
    """Record the event for ``task`` in the outbox, in the caller's transaction.

    ``pg_notify`` is transactional: Postgres delivers the notification only when the transaction
    commits and drops it on rollback, so it is a safe low-latency wake-up for the relay.
    """
    session.add(
        OutboxEvent(
            task_id=task.id,
            routing_key=task_routing_key(task.type),
            payload=task_event_payload(task),
            created_at=task.created_at,
        )
    )
    await session.execute(text(f"SELECT pg_notify('{OUTBOX_NOTIFY_CHANNEL}', '')"))
