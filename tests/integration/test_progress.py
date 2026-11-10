"""Applying worker progress: idempotency, order tolerance, tenant transitions, resilience."""

import json
import uuid
from typing import Any

import aio_pika
import httpx
import pytest
from sqlalchemy.exc import OperationalError

from app.db.models import ProcessedMessage
from app.messaging.progress_consumer import ProgressConsumer
from app.messaging.topology import Topology
from app.services.progress_service import ApplyOutcome, UnknownTaskError
from tests.conftest import eventually
from tests.helpers import apply, count, finish_task, get_task, get_tenant, progress

pytestmark = pytest.mark.asyncio


async def new_tenant(client: httpx.AsyncClient, slug: str = "acme") -> tuple[str, str]:
    body = (await client.post("/v1/tenants", json={"slug": slug, "name": "A"})).json()
    return body["tenant"]["id"], body["task"]["id"]


# --- service level --------------------------------------------------------------------------


async def test_deploy_lifecycle(client: httpx.AsyncClient, sessionmaker) -> None:
    tenant_id, task_id = await new_tenant(client)

    assert await apply(sessionmaker, progress(task_id, "in_progress")) == ApplyOutcome.APPLIED
    assert (await get_task(sessionmaker, task_id)).status == "in_progress"
    tenant = await get_tenant(sessionmaker, tenant_id)
    assert (tenant.status, tenant.version) == ("provisioning", 1)  # non-terminal: tenant unchanged

    assert await apply(sessionmaker, progress(task_id, "done")) == ApplyOutcome.APPLIED
    assert (await get_task(sessionmaker, task_id)).status == "done"
    tenant = await get_tenant(sessionmaker, tenant_id)
    assert (tenant.status, tenant.version) == ("active", 2)


async def test_failure_moves_tenant_to_failed(client: httpx.AsyncClient, sessionmaker) -> None:
    tenant_id, task_id = await new_tenant(client)
    await apply(sessionmaker, progress(task_id, "failed", error="disk full"))
    task = await get_task(sessionmaker, task_id)
    assert (task.status, task.error) == ("failed", "disk full")
    tenant = await get_tenant(sessionmaker, tenant_id)
    assert (tenant.status, tenant.version) == ("failed", 2)


@pytest.mark.parametrize(("outcome", "expected"), [("done", "active"), ("failed", "failed")])
async def test_update_task_outcomes(
    client: httpx.AsyncClient, sessionmaker, outcome: str, expected: str
) -> None:
    tenant_id, task_id = await new_tenant(client)
    await finish_task(sessionmaker, task_id)
    upd = (await client.patch(f"/v1/tenants/{tenant_id}", json={"name": "B", "version": 2})).json()
    await finish_task(sessionmaker, upd["task"]["id"], outcome)
    tenant = await get_tenant(sessionmaker, tenant_id)
    assert (tenant.status, tenant.version) == (expected, 4)


@pytest.mark.parametrize(("outcome", "expected"), [("done", "destroyed"), ("failed", "failed")])
async def test_destroy_task_outcomes(
    client: httpx.AsyncClient, sessionmaker, outcome: str, expected: str
) -> None:
    tenant_id, task_id = await new_tenant(client)
    await finish_task(sessionmaker, task_id)
    destroy = (await client.delete(f"/v1/tenants/{tenant_id}")).json()
    await finish_task(sessionmaker, destroy["task"]["id"], outcome)
    assert (await get_tenant(sessionmaker, tenant_id)).status == expected


async def test_duplicate_is_a_no_op(client: httpx.AsyncClient, sessionmaker) -> None:
    tenant_id, task_id = await new_tenant(client)
    done = progress(task_id, "done")
    assert await apply(sessionmaker, done) == ApplyOutcome.APPLIED
    before = await get_tenant(sessionmaker, tenant_id)
    for _ in range(3):
        assert await apply(sessionmaker, done) == ApplyOutcome.DUPLICATE
    after = await get_tenant(sessionmaker, tenant_id)
    assert (after.status, after.version, after.updated_at) == (
        before.status,
        before.version,
        before.updated_at,
    )


async def test_stale_in_progress_after_done(client: httpx.AsyncClient, sessionmaker) -> None:
    tenant_id, task_id = await new_tenant(client)
    assert await apply(sessionmaker, progress(task_id, "done")) == ApplyOutcome.APPLIED
    assert await apply(sessionmaker, progress(task_id, "in_progress")) == ApplyOutcome.STALE
    assert (await get_task(sessionmaker, task_id)).status == "done"
    tenant = await get_tenant(sessionmaker, tenant_id)
    assert (tenant.status, tenant.version) == ("active", 2)


async def test_terminal_task_ignores_other_outcome(client: httpx.AsyncClient, sessionmaker) -> None:
    tenant_id, task_id = await new_tenant(client)
    await apply(sessionmaker, progress(task_id, "done"))
    assert await apply(sessionmaker, progress(task_id, "failed")) == ApplyOutcome.STALE
    assert (await get_task(sessionmaker, task_id)).status == "done"
    assert (await get_tenant(sessionmaker, tenant_id)).status == "active"


async def test_late_update_for_old_task_does_not_touch_tenant(
    client: httpx.AsyncClient, sessionmaker
) -> None:
    """A stale message about the finished deploy must not affect the tenant's later update."""
    tenant_id, deploy_id = await new_tenant(client)
    await finish_task(sessionmaker, deploy_id)
    await client.patch(f"/v1/tenants/{tenant_id}", json={"name": "B", "version": 2})
    assert await apply(sessionmaker, progress(deploy_id, "failed")) == ApplyOutcome.STALE
    tenant = await get_tenant(sessionmaker, tenant_id)
    assert (tenant.status, tenant.version) == ("updating", 3)


async def test_unknown_task_is_rejected(sessionmaker) -> None:
    with pytest.raises(UnknownTaskError):
        await apply(sessionmaker, progress(uuid.uuid4(), "done"))
    assert await count(sessionmaker, ProcessedMessage) == 0


# --- consumer level: ack / reject / retry decisions ----------------------------------------


class FakeMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.message_id = "fake"
        self.result: tuple[str, bool | None] | None = None

    async def ack(self) -> None:
        self.result = ("ack", None)

    async def reject(self, requeue: bool = False) -> None:
        self.result = ("reject", requeue)

    async def nack(self, requeue: bool = True) -> None:
        self.result = ("nack", requeue)


def body(update: Any) -> bytes:
    return json.dumps(update.model_dump(mode="json")).encode()


def consumer(sm: Any, **kw: Any) -> ProgressConsumer:
    return ProgressConsumer(sm, queue=None, backoff_multiplier_s=0.001, **kw)  # type: ignore[arg-type]


async def test_handle_acks_applied_duplicate_and_stale(
    client: httpx.AsyncClient, sessionmaker
) -> None:
    _, task_id = await new_tenant(client)
    c = consumer(sessionmaker)
    done = progress(task_id, "done")
    for update in (done, done, progress(task_id, "in_progress")):
        msg = FakeMessage(body(update))
        await c.handle(msg)  # type: ignore[arg-type]
        assert msg.result == ("ack", None)


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        b"{}",
        b'{"message_id": "x", "task_id": "y", "status": "done", "occurred_at": "z"}',
        json.dumps(
            {
                "message_id": str(uuid.uuid4()),
                "task_id": str(uuid.uuid4()),
                "status": "exploded",
                "occurred_at": "2026-01-01T00:00:00Z",
            }
        ).encode(),
    ],
)
async def test_handle_rejects_malformed_without_requeue(sessionmaker, raw: bytes) -> None:
    msg = FakeMessage(raw)
    await consumer(sessionmaker).handle(msg)  # type: ignore[arg-type]
    assert msg.result == ("reject", False)


async def test_handle_rejects_unknown_task(sessionmaker) -> None:
    msg = FakeMessage(body(progress(uuid.uuid4(), "done")))
    await consumer(sessionmaker).handle(msg)  # type: ignore[arg-type]
    assert msg.result == ("reject", False)


class FlakySessionmaker:
    """Raises a transient DB error for the first ``failures`` sessions."""

    def __init__(self, real: Any, failures: int) -> None:
        self.real = real
        self.failures = failures
        self.calls = 0

    def __call__(self) -> Any:
        self.calls += 1
        if self.calls <= self.failures:
            raise OperationalError("connect", {}, ConnectionRefusedError("db down"))
        return self.real()


async def test_transient_errors_are_retried(client: httpx.AsyncClient, sessionmaker) -> None:
    tenant_id, task_id = await new_tenant(client)
    flaky = FlakySessionmaker(sessionmaker, failures=3)
    msg = FakeMessage(body(progress(task_id, "done")))
    await consumer(flaky, max_attempts=5).handle(msg)  # type: ignore[arg-type]
    assert msg.result == ("ack", None)
    assert flaky.calls == 4
    assert (await get_tenant(sessionmaker, tenant_id)).status == "active"


async def test_persistent_errors_are_requeued(client: httpx.AsyncClient, sessionmaker) -> None:
    _, task_id = await new_tenant(client)
    flaky = FlakySessionmaker(sessionmaker, failures=100)
    msg = FakeMessage(body(progress(task_id, "done")))
    await consumer(flaky, max_attempts=3).handle(msg)  # type: ignore[arg-type]
    assert msg.result == ("nack", True)  # broker redelivers; the delivery limit bounds it
    assert flaky.calls == 3


# --- consumer level: real broker ------------------------------------------------------------


async def test_poison_goes_to_dlq_and_consumer_keeps_running(
    client: httpx.AsyncClient, sessionmaker, topology: Topology, channel
) -> None:
    tenant_id, task_id = await new_tenant(client)
    queue = await channel.get_queue(topology.updates_queue.name)
    c = ProgressConsumer(sessionmaker, queue)
    await c.start()
    try:
        publish = topology.updates_exchange.publish
        await publish(aio_pika.Message(b"garbage", message_id="poison-1"), "task-update.done")
        await publish(
            aio_pika.Message(body(progress(uuid.uuid4(), "done")), message_id="poison-2"),
            "task-update.done",
        )
        good = progress(task_id, "done")
        await publish(aio_pika.Message(body(good)), "task-update.done")
        await publish(aio_pika.Message(body(good)), "task-update.done")  # duplicate delivery

        async def tenant_active() -> bool:
            return (await get_tenant(sessionmaker, tenant_id)).status == "active"

        await eventually(tenant_active)

        dead: list[str] = []

        async def two_dead_letters() -> bool:
            msg = await topology.updates_dlq.get(fail=False, no_ack=True)
            if msg is not None:
                dead.append(str(msg.message_id))
            return len(dead) == 2

        await eventually(two_dead_letters)
        assert sorted(dead) == ["poison-1", "poison-2"]
        assert (await get_tenant(sessionmaker, tenant_id)).version == 2
    finally:
        await c.stop()
