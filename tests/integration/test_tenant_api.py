import re
import uuid

import httpx
import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import Tenant
from tests.helpers import finish_task, outbox_rows

TS = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$")

pytestmark = pytest.mark.asyncio


async def create(client: httpx.AsyncClient, slug: str = "acme", name: str = "Acme") -> dict:
    r = await client.post("/v1/tenants", json={"slug": slug, "name": name})
    assert r.status_code == 201, r.text
    return r.json()


async def active_tenant(
    client: httpx.AsyncClient, sm: async_sessionmaker[AsyncSession], slug: str = "acme"
) -> dict:
    created = await create(client, slug)
    await finish_task(sm, created["task"]["id"])
    r = await client.get(f"/v1/tenants/{created['tenant']['id']}")
    assert r.json()["status"] == "active"
    return r.json()


def assert_error(r: httpx.Response, status: int, code: str) -> dict:
    assert r.status_code == status, r.text
    body = r.json()
    assert set(body) == {"error"}
    assert body["error"]["code"] == code
    assert isinstance(body["error"]["message"], str)
    assert isinstance(body["error"]["details"], dict)
    return body["error"]


# --- create ---------------------------------------------------------------------------------


async def test_create_tenant(client: httpx.AsyncClient, sessionmaker) -> None:
    r = await client.post("/v1/tenants", json={"slug": "acme-corp", "name": "Acme Corp"})
    assert r.status_code == 201
    body = r.json()
    tenant, task = body["tenant"], body["task"]
    assert r.headers["location"] == f"/v1/tenants/{tenant['id']}"
    assert tenant["slug"] == "acme-corp"
    assert tenant["name"] == "Acme Corp"
    assert tenant["status"] == "provisioning"
    assert tenant["version"] == 1
    assert TS.match(tenant["created_at"])
    assert TS.match(tenant["updated_at"])
    assert task["type"] == "deploy"
    assert task["status"] == "accepted"
    assert task["tenant_id"] == tenant["id"]
    assert TS.match(task["created_at"])

    # The event was written to the outbox in the same transaction, and carries the task.
    [event] = await outbox_rows(sessionmaker)
    assert event.published_at is None
    assert event.routing_key == "task.deploy"
    assert event.payload == {
        "id": task["id"],
        "type": "deploy",
        "tenant_id": tenant["id"],
        "status": "accepted",
        "created_at": task["created_at"],
    }


async def test_duplicate_slug(client: httpx.AsyncClient, sessionmaker) -> None:
    await create(client, "acme")
    r = await client.post("/v1/tenants", json={"slug": "acme", "name": "Other"})
    err = assert_error(r, 409, "tenant_already_exists")
    assert err["details"] == {"slug": "acme"}
    assert len(await outbox_rows(sessionmaker)) == 1  # the failed create emitted nothing


async def test_slug_stays_reserved_after_destroy(client: httpx.AsyncClient, sessionmaker) -> None:
    tenant = await active_tenant(client, sessionmaker)
    deleted = (await client.delete(f"/v1/tenants/{tenant['id']}")).json()
    await finish_task(sessionmaker, deleted["task"]["id"])
    assert (await client.get(f"/v1/tenants/{tenant['id']}")).json()["status"] == "destroyed"

    r = await client.post("/v1/tenants", json={"slug": "acme", "name": "Again"})
    assert_error(r, 409, "tenant_already_exists")


@pytest.mark.parametrize(
    "payload",
    [
        {"slug": "ab", "name": "x"},
        {"slug": "Bad_Slug", "name": "x"},
        {"slug": "acme", "name": ""},
        {"slug": "acme"},
        {"slug": "acme", "name": "x", "unexpected": True},
    ],
)
async def test_create_validation(client: httpx.AsyncClient, payload: dict) -> None:
    err = assert_error(await client.post("/v1/tenants", json=payload), 422, "validation_error")
    assert err["details"]["errors"]


async def test_malformed_json(client: httpx.AsyncClient) -> None:
    r = await client.post(
        "/v1/tenants", content=b"{not json", headers={"content-type": "application/json"}
    )
    assert_error(r, 422, "validation_error")


# --- read -----------------------------------------------------------------------------------


async def test_get_tenant_not_found(client: httpx.AsyncClient) -> None:
    assert_error(await client.get(f"/v1/tenants/{uuid.uuid4()}"), 404, "tenant_not_found")
    assert_error(await client.get("/v1/tenants/not-a-uuid"), 404, "tenant_not_found")


async def test_get_task(client: httpx.AsyncClient) -> None:
    created = await create(client)
    r = await client.get(f"/v1/tasks/{created['task']['id']}")
    assert r.status_code == 200
    assert r.json() == created["task"]
    assert_error(await client.get(f"/v1/tasks/{uuid.uuid4()}"), 404, "task_not_found")
    assert_error(await client.get("/v1/tasks/nope"), 404, "task_not_found")


async def test_list_tenants_pagination(client: httpx.AsyncClient) -> None:
    slugs = [f"tenant-{i}" for i in range(5)]
    for slug in slugs:
        await create(client, slug)

    seen: list[str] = []
    cursor = None
    pages = 0
    while True:
        params = {"limit": 2} | ({"cursor": cursor} if cursor else {})
        body = (await client.get("/v1/tenants", params=params)).json()
        seen += [t["slug"] for t in body["items"]]
        pages += 1
        cursor = body["next_cursor"]
        if cursor is None:
            break
    assert pages == 3
    assert seen == list(reversed(slugs))  # newest first, no gaps or duplicates


async def test_list_tenants_filter_and_errors(client: httpx.AsyncClient, sessionmaker) -> None:
    await active_tenant(client, sessionmaker, "alpha")
    await create(client, "beta")
    body = (await client.get("/v1/tenants", params={"status": "active"})).json()
    assert [t["slug"] for t in body["items"]] == ["alpha"]

    assert_error(
        await client.get("/v1/tenants", params={"status": "bogus"}), 422, "validation_error"
    )
    assert_error(await client.get("/v1/tenants", params={"limit": 0}), 422, "validation_error")
    assert_error(await client.get("/v1/tenants", params={"limit": 101}), 422, "validation_error")
    assert_error(await client.get("/v1/tenants", params={"cursor": "zzz"}), 422, "invalid_cursor")


async def test_list_tasks_filters(client: httpx.AsyncClient, sessionmaker) -> None:
    a = await active_tenant(client, sessionmaker, "alpha")
    b = await create(client, "beta")
    await client.patch(f"/v1/tenants/{a['id']}", json={"name": "A2", "version": a["version"]})

    all_tasks = (await client.get("/v1/tasks")).json()["items"]
    assert [t["type"] for t in all_tasks] == ["update", "deploy", "deploy"]

    by_tenant = (await client.get("/v1/tasks", params={"tenant_id": a["id"]})).json()["items"]
    assert {t["type"] for t in by_tenant} == {"deploy", "update"}

    nested = (await client.get(f"/v1/tenants/{a['id']}/tasks")).json()["items"]
    assert nested == by_tenant

    accepted = (await client.get("/v1/tasks", params={"status": "accepted"})).json()["items"]
    assert {t["tenant_id"] for t in accepted} == {a["id"], b["tenant"]["id"]}

    updates = (await client.get("/v1/tasks", params={"type": "update"})).json()["items"]
    assert len(updates) == 1

    assert_error(await client.get(f"/v1/tenants/{uuid.uuid4()}/tasks"), 404, "tenant_not_found")
    assert_error(await client.get("/v1/tasks", params={"tenant_id": "x"}), 422, "validation_error")


# --- update ---------------------------------------------------------------------------------


async def test_update_tenant(client: httpx.AsyncClient, sessionmaker) -> None:
    tenant = await active_tenant(client, sessionmaker)
    assert tenant["version"] == 2  # create=1, worker-applied deploy done=2

    r = await client.patch(f"/v1/tenants/{tenant['id']}", json={"name": "New", "version": 2})
    assert r.status_code == 202
    body = r.json()
    assert body["tenant"]["status"] == "updating"
    assert body["tenant"]["name"] == "New"
    assert body["tenant"]["version"] == 3
    assert body["task"]["type"] == "update"
    assert body["task"]["status"] == "accepted"
    assert [e.routing_key for e in await outbox_rows(sessionmaker)] == [
        "task.deploy",
        "task.update",
    ]

    await finish_task(sessionmaker, body["task"]["id"])
    after = (await client.get(f"/v1/tenants/{tenant['id']}")).json()
    assert after["status"] == "active"
    assert after["version"] == 4


async def test_update_stale_version(client: httpx.AsyncClient, sessionmaker) -> None:
    tenant = await active_tenant(client, sessionmaker)
    r = await client.patch(f"/v1/tenants/{tenant['id']}", json={"name": "New", "version": 1})
    err = assert_error(r, 409, "tenant_version_conflict")
    assert err["details"] == {"expected_version": 1, "current_version": 2}
    assert (await client.get(f"/v1/tenants/{tenant['id']}")).json() == tenant  # unchanged


@pytest.mark.parametrize(
    "status", ["provisioning", "updating", "destroying", "destroyed", "failed"]
)
async def test_update_not_allowed_outside_active(
    client: httpx.AsyncClient, sessionmaker: async_sessionmaker[AsyncSession], status: str
) -> None:
    created = await create(client)
    tid = created["tenant"]["id"]
    async with sessionmaker() as s, s.begin():
        await s.execute(update(Tenant).where(Tenant.id == uuid.UUID(tid)).values(status=status))
    r = await client.patch(f"/v1/tenants/{tid}", json={"name": "New", "version": 1})
    err = assert_error(r, 409, "tenant_update_not_allowed")
    assert err["details"]["status"] == status
    assert len(await outbox_rows(sessionmaker)) == 1  # no update event emitted


async def test_update_errors(client: httpx.AsyncClient) -> None:
    missing = f"/v1/tenants/{uuid.uuid4()}"
    assert_error(
        await client.patch(missing, json={"name": "x", "version": 1}), 404, "tenant_not_found"
    )
    created = await create(client)
    path = f"/v1/tenants/{created['tenant']['id']}"
    assert_error(await client.patch(path, json={"name": "x"}), 422, "validation_error")
    assert_error(await client.patch(path, json={"version": 1}), 422, "validation_error")
    assert_error(
        await client.patch(path, json={"slug": "new", "version": 1}), 422, "validation_error"
    )


# --- delete ---------------------------------------------------------------------------------


async def test_delete_active_tenant(client: httpx.AsyncClient, sessionmaker) -> None:
    tenant = await active_tenant(client, sessionmaker)
    r = await client.delete(f"/v1/tenants/{tenant['id']}")
    assert r.status_code == 202
    body = r.json()
    assert body["tenant"]["status"] == "destroying"
    assert body["tenant"]["version"] == 3
    assert body["task"]["type"] == "destroy"

    await finish_task(sessionmaker, body["task"]["id"])
    after = (await client.get(f"/v1/tenants/{tenant['id']}")).json()
    assert after["status"] == "destroyed"
    assert after["version"] == 4


async def test_delete_failed_tenant(client: httpx.AsyncClient, sessionmaker) -> None:
    created = await create(client)
    await finish_task(sessionmaker, created["task"]["id"], "failed")
    tid = created["tenant"]["id"]
    assert (await client.get(f"/v1/tenants/{tid}")).json()["status"] == "failed"
    r = await client.delete(f"/v1/tenants/{tid}")
    assert r.status_code == 202
    assert r.json()["tenant"]["status"] == "destroying"


async def test_delete_not_allowed(client: httpx.AsyncClient, sessionmaker) -> None:
    created = await create(client)
    tid = created["tenant"]["id"]
    # provisioning
    err = assert_error(await client.delete(f"/v1/tenants/{tid}"), 409, "tenant_update_not_allowed")
    assert err["details"]["status"] == "provisioning"
    # destroying
    await finish_task(sessionmaker, created["task"]["id"])
    destroy = (await client.delete(f"/v1/tenants/{tid}")).json()
    assert_error(await client.delete(f"/v1/tenants/{tid}"), 409, "tenant_update_not_allowed")
    # destroyed
    await finish_task(sessionmaker, destroy["task"]["id"])
    assert_error(await client.delete(f"/v1/tenants/{tid}"), 409, "tenant_update_not_allowed")
    assert_error(await client.delete(f"/v1/tenants/{uuid.uuid4()}"), 404, "tenant_not_found")


# --- misc -----------------------------------------------------------------------------------


async def test_unknown_route_and_method(client: httpx.AsyncClient) -> None:
    assert_error(await client.get("/v1/nope"), 404, "not_found")
    assert_error(await client.put("/v1/tenants"), 405, "method_not_allowed")
    # tasks are read-only over HTTP
    assert_error(await client.post("/v1/tasks", json={}), 405, "method_not_allowed")


async def test_healthz(client: httpx.AsyncClient) -> None:
    r = await client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}
