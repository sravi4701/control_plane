"""End to end: the real app (HTTP API + outbox relay + progress consumer, started by its own
lifespan) and the real worker simulator, talking through RabbitMQ and Postgres. Nothing is
driven by hand: the only inputs are HTTP calls and the worker's configuration."""

import random
from collections import Counter
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from aio_pika.abc import AbstractRobustConnection
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.db.models import ProcessedMessage
from app.main import create_app
from app.messaging.topology import Topology, declare_topology
from tests.conftest import eventually
from worker.simulator import WorkerConfig, WorkerSimulator

pytestmark = pytest.mark.asyncio

FAST = {"min_delay_ms": 5, "max_delay_ms": 30}

WorkerFactory = Callable[..., Any]


@pytest.fixture
async def api(settings: Settings, topology: Topology) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings.model_copy(update={"run_background_workers": True}))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest.fixture
def worker(amqp_connection: AbstractRobustConnection) -> WorkerFactory:
    """Start a worker simulator; leaving the context stops it (like stopping the container)."""

    @asynccontextmanager
    async def start(**config: Any) -> AsyncIterator[None]:
        channel = await amqp_connection.channel(publisher_confirms=True, on_return_raises=True)
        await channel.set_qos(prefetch_count=4)
        topo = await declare_topology(channel)
        sim = WorkerSimulator(
            WorkerConfig(**(FAST | config)), topo.updates_exchange, random.Random(7)
        )
        await topo.tasks_queue.consume(sim.handle)
        try:
            yield
        finally:
            await channel.close()

    return start


async def wait_for_status(api: httpx.AsyncClient, tenant_id: str, status: str) -> dict:
    async def probe() -> dict | None:
        tenant = (await api.get(f"/v1/tenants/{tenant_id}")).json()
        return tenant if tenant["status"] == status else None

    return await eventually(probe, timeout=15)


async def create(api: httpx.AsyncClient, slug: str) -> dict:
    r = await api.post("/v1/tenants", json={"slug": slug, "name": slug.title()})
    assert r.status_code == 201
    return r.json()


async def test_full_lifecycle(api: httpx.AsyncClient, worker: WorkerFactory) -> None:
    async with worker():
        created = await create(api, "acme")
        tid = created["tenant"]["id"]
        tenant = await wait_for_status(api, tid, "active")
        assert tenant["version"] == 2

        [deploy] = (await api.get("/v1/tasks", params={"tenant_id": tid})).json()["items"]
        assert (deploy["type"], deploy["status"]) == ("deploy", "done")

        r = await api.patch(f"/v1/tenants/{tid}", json={"name": "Acme 2", "version": 2})
        assert r.status_code == 202
        tenant = await wait_for_status(api, tid, "active")
        assert (tenant["name"], tenant["version"]) == ("Acme 2", 4)

        assert (await api.delete(f"/v1/tenants/{tid}")).status_code == 202
        tenant = await wait_for_status(api, tid, "destroyed")
        assert tenant["version"] == 6

        tasks = (await api.get(f"/v1/tenants/{tid}/tasks")).json()["items"]
        assert [(t["type"], t["status"]) for t in tasks] == [
            ("destroy", "done"),
            ("update", "done"),
            ("deploy", "done"),
        ]


async def test_failure_then_recovery_by_restarting_worker(
    api: httpx.AsyncClient, worker: WorkerFactory
) -> None:
    async with worker(fail_rate=1.0):
        created = await create(api, "doomed")
        tid = created["tenant"]["id"]
        await wait_for_status(api, tid, "failed")
        task = (await api.get(f"/v1/tasks/{created['task']['id']}")).json()
        assert task["status"] == "failed"
        assert task["error"]

    async with worker(fail_rate=0.0):  # "restart the worker with different args"
        assert (await api.delete(f"/v1/tenants/{tid}")).status_code == 202
        await wait_for_status(api, tid, "destroyed")


async def test_duplicates_and_reordering_converge(
    api: httpx.AsyncClient,
    worker: WorkerFactory,
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    async with worker(duplicate_rate=1.0, reorder=True):
        tenants = [await create(api, f"chaos-{i}") for i in range(5)]
        for t in tenants:
            final = await wait_for_status(api, t["tenant"]["id"], "active")
            assert final["version"] == 2  # moved exactly once despite 4 deliveries

        async def all_recorded() -> list[ProcessedMessage] | None:
            async with sessionmaker() as s:
                rows = list((await s.scalars(select(ProcessedMessage))).all())
            return rows if len(rows) == 2 * len(tenants) else None

        # 4 deliveries per task, but only 2 distinct message ids: duplicates were dropped.
        rows = await eventually(all_recorded)
        # The consumer handles messages concurrently, so a reordered in_progress is either
        # stale (arrived after done) or a harmless forward step (processed first). Either way
        # every terminal update applied, and the tenant moved exactly once (version 2 above).
        assert {r.outcome for r in rows} <= {"applied", "stale"}
        assert Counter(r.outcome for r in rows)["applied"] >= len(tenants)
        for t in tenants:
            task = (await api.get(f"/v1/tasks/{t['task']['id']}")).json()
            assert task["status"] == "done"


async def test_many_tenants_concurrently(api: httpx.AsyncClient, worker: WorkerFactory) -> None:
    async with worker(fail_rate=0.5):
        tenants = [await create(api, f"bulk-{i:02d}") for i in range(20)]

        async def all_settled() -> list[dict] | None:
            body = (await api.get("/v1/tenants", params={"limit": 100})).json()
            statuses = {t["status"] for t in body["items"]}
            return body["items"] if statuses <= {"active", "failed"} else None

        items = await eventually(all_settled, timeout=20)
        assert len(items) == len(tenants)
        assert all(t["version"] == 2 for t in items)
