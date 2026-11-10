"""Publish-after-commit: events reach the broker only for committed transactions."""

import asyncio
import json
import uuid
from typing import Any

import httpx
import pytest
from aio_pika.abc import AbstractQueue
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import Task, Tenant
from app.domain.state_machine import TaskStatus, TaskType
from app.messaging.broker import AmqpPublisher
from app.messaging.outbox import enqueue_task_event
from app.messaging.outbox_relay import OutboxRelay
from app.messaging.schemas import TaskEvent
from app.messaging.topology import Topology
from app.services.tenant_service import create_tenant
from app.timeutil import utcnow
from tests.conftest import eventually
from tests.helpers import outbox_rows

pytestmark = pytest.mark.asyncio


async def drain(queue: AbstractQueue, *, settle: float = 0.3) -> list[Any]:
    """Every message currently in ``queue`` (acked), waiting briefly for in-flight ones."""
    await asyncio.sleep(settle)
    messages = []
    while (msg := await queue.get(fail=False, no_ack=True)) is not None:
        messages.append(msg)
    return messages


def relay(sm: async_sessionmaker[AsyncSession], topology: Topology, **kw: Any) -> OutboxRelay:
    return OutboxRelay(sm, AmqpPublisher(topology.tasks_exchange), **kw)


async def test_committed_event_is_published_once(
    client: httpx.AsyncClient, sessionmaker, topology: Topology
) -> None:
    created = (await client.post("/v1/tenants", json={"slug": "acme", "name": "A"})).json()
    r = relay(sessionmaker, topology)
    assert await r.publish_pending() == 1
    assert await r.publish_pending() == 0  # marked published; not sent again

    [msg] = await drain(topology.tasks_queue)
    event = TaskEvent.model_validate_json(msg.body)
    task = created["task"]
    assert (str(event.id), event.type, str(event.tenant_id), event.status) == (
        task["id"],
        "deploy",
        task["tenant_id"],
        "accepted",
    )
    assert msg.message_id == task["id"]  # task id doubles as the event / AMQP message id
    assert msg.delivery_mode == 2  # persistent
    assert msg.routing_key == "task.deploy"
    [row] = await outbox_rows(sessionmaker)
    assert row.published_at is not None


async def test_rolled_back_transaction_publishes_nothing(
    sessionmaker: async_sessionmaker[AsyncSession], topology: Topology
) -> None:
    class SimulatedCrashError(Exception):
        pass

    async def write_then_crash() -> None:
        async with sessionmaker() as session, session.begin():
            now = utcnow()
            tenant = Tenant(
                id=uuid.uuid4(),
                slug="ghost",
                name="g",
                status="provisioning",
                version=1,
                created_at=now,
                updated_at=now,
            )
            session.add(tenant)
            await session.flush()
            task = Task(
                id=uuid.uuid4(),
                type=TaskType.DEPLOY,
                tenant_id=tenant.id,
                status=TaskStatus.ACCEPTED,
                created_at=now,
                updated_at=now,
            )
            session.add(task)
            await session.flush()
            await enqueue_task_event(session, task)
            raise SimulatedCrashError  # e.g. a failure after the writes, before commit

    with pytest.raises(SimulatedCrashError):
        await write_then_crash()

    assert await outbox_rows(sessionmaker) == []
    assert await relay(sessionmaker, topology).publish_pending() == 0
    assert await drain(topology.tasks_queue) == []


async def test_failed_create_publishes_nothing(
    sessionmaker: async_sessionmaker[AsyncSession], topology: Topology
) -> None:
    async with sessionmaker() as s:
        await create_tenant(s, "acme", "A")
    async with sessionmaker() as s:
        with pytest.raises(Exception, match="already exists"):
            await create_tenant(s, "acme", "B")
    assert await relay(sessionmaker, topology).publish_pending() == 1
    assert len(await drain(topology.tasks_queue)) == 1


async def test_publish_failure_keeps_rows_for_retry(
    client: httpx.AsyncClient, sessionmaker, topology: Topology
) -> None:
    await client.post("/v1/tenants", json={"slug": "one", "name": "1"})
    await client.post("/v1/tenants", json={"slug": "two", "name": "2"})

    class FlakyPublisher:
        def __init__(self) -> None:
            self.calls = 0

        async def publish(self, routing_key: str, payload: dict, message_id: str) -> None:
            self.calls += 1
            if self.calls == 2:
                raise ConnectionError("broker went away")

    with pytest.raises(ConnectionError):
        await OutboxRelay(sessionmaker, FlakyPublisher()).publish_pending()
    # The whole batch rolled back: nothing is marked published, so nothing is lost.
    assert all(row.published_at is None for row in await outbox_rows(sessionmaker))

    assert await relay(sessionmaker, topology).publish_pending() == 2
    assert len(await drain(topology.tasks_queue)) == 2


async def test_relay_is_woken_by_notify_on_commit(
    client: httpx.AsyncClient, sessionmaker, settings, topology: Topology
) -> None:
    """With a 60s poll interval, only the post-commit NOTIFY can deliver the event quickly."""
    r = relay(sessionmaker, topology, poll_interval_s=60, listen_database_url=settings.database_url)
    task = asyncio.create_task(r.run())
    try:
        await asyncio.sleep(0.5)  # let the relay finish its first (empty) pass and LISTEN
        await client.post("/v1/tenants", json={"slug": "fast", "name": "F"})
        msg = await eventually(lambda: topology.tasks_queue.get(fail=False, no_ack=True), timeout=5)
        assert json.loads(msg.body)["type"] == "deploy"
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
