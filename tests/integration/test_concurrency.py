"""Race conditions, exercised for real: N requests fired concurrently, each on its own DB
connection, against real Postgres. The assertions are the invariants, not timings."""

import asyncio
import uuid
from collections import Counter

import httpx
import pytest

from app.db.models import OutboxEvent, ProcessedMessage, Task, Tenant
from app.services.progress_service import ApplyOutcome
from tests.helpers import apply, count, finish_task, get_tenant, progress

pytestmark = pytest.mark.asyncio

N = 25


def codes(responses: list[httpx.Response]) -> Counter[tuple[int, str | None]]:
    return Counter(
        (r.status_code, r.json().get("error", {}).get("code") if r.status_code >= 400 else None)
        for r in responses
    )


async def test_concurrent_creates_same_slug(client: httpx.AsyncClient, sessionmaker) -> None:
    responses = await asyncio.gather(
        *(client.post("/v1/tenants", json={"slug": "race", "name": f"n{i}"}) for i in range(N))
    )
    assert codes(responses) == {(201, None): 1, (409, "tenant_already_exists"): N - 1}
    assert await count(sessionmaker, Tenant) == 1
    assert await count(sessionmaker, Task) == 1
    assert await count(sessionmaker, OutboxEvent) == 1


async def _active(client: httpx.AsyncClient, sessionmaker) -> dict:
    created = (await client.post("/v1/tenants", json={"slug": "race", "name": "r"})).json()
    await finish_task(sessionmaker, created["task"]["id"])
    return (await client.get(f"/v1/tenants/{created['tenant']['id']}")).json()


async def test_concurrent_patches_same_version(client: httpx.AsyncClient, sessionmaker) -> None:
    tenant = await _active(client, sessionmaker)
    responses = await asyncio.gather(
        *(
            client.patch(
                f"/v1/tenants/{tenant['id']}", json={"name": f"n{i}", "version": tenant["version"]}
            )
            for i in range(N)
        )
    )
    assert codes(responses) == {(202, None): 1, (409, "tenant_version_conflict"): N - 1}
    [winner] = [r.json() for r in responses if r.status_code == 202]
    final = await get_tenant(sessionmaker, tenant["id"])
    assert final.version == tenant["version"] + 1
    assert final.name == winner["tenant"]["name"]
    assert await count(sessionmaker, Task) == 2  # deploy + exactly one update
    assert await count(sessionmaker, OutboxEvent) == 2


async def test_concurrent_deletes(client: httpx.AsyncClient, sessionmaker) -> None:
    tenant = await _active(client, sessionmaker)
    responses = await asyncio.gather(
        *(client.delete(f"/v1/tenants/{tenant['id']}") for _ in range(N))
    )
    assert codes(responses) == {(202, None): 1, (409, "tenant_update_not_allowed"): N - 1}
    assert await count(sessionmaker, Task) == 2


async def test_concurrent_patch_and_delete(client: httpx.AsyncClient, sessionmaker) -> None:
    """Different operations racing on one tenant: exactly one wins, so at most one open task."""
    tenant = await _active(client, sessionmaker)
    path = f"/v1/tenants/{tenant['id']}"
    responses = await asyncio.gather(
        *(
            client.patch(path, json={"name": "x", "version": tenant["version"]})
            if i % 2
            else client.delete(path)
            for i in range(N)
        )
    )
    assert sum(r.status_code == 202 for r in responses) == 1
    assert all(r.status_code in {202, 409} for r in responses)
    assert await count(sessionmaker, Task) == 2


async def test_concurrent_duplicate_progress_updates(
    client: httpx.AsyncClient, sessionmaker
) -> None:
    """The same update delivered N times at once is applied exactly once."""
    created = (await client.post("/v1/tenants", json={"slug": "race", "name": "r"})).json()
    task_id = created["task"]["id"]
    await apply(sessionmaker, progress(task_id, "in_progress"))
    message_id = uuid.uuid4()
    outcomes = await asyncio.gather(
        *(apply(sessionmaker, progress(task_id, "done", message_id=message_id)) for _ in range(N))
    )
    assert Counter(outcomes) == {ApplyOutcome.APPLIED: 1, ApplyOutcome.DUPLICATE: N - 1}
    tenant = await get_tenant(sessionmaker, created["tenant"]["id"])
    assert (tenant.status, tenant.version) == ("active", 2)
    assert await count(sessionmaker, ProcessedMessage) == 2


async def test_concurrent_conflicting_outcomes(client: httpx.AsyncClient, sessionmaker) -> None:
    """`done` and `failed` racing for the same task: the first wins, the other is stale."""
    created = (await client.post("/v1/tenants", json={"slug": "race", "name": "r"})).json()
    task_id = created["task"]["id"]
    outcomes = await asyncio.gather(
        *(apply(sessionmaker, progress(task_id, "done" if i % 2 else "failed")) for i in range(N))
    )
    assert Counter(outcomes)[ApplyOutcome.APPLIED] == 1
    tenant = await get_tenant(sessionmaker, created["tenant"]["id"])
    assert tenant.version == 2  # moved exactly once
    assert tenant.status in {"active", "failed"}
