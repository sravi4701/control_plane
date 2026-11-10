"""Concurrency demo against the *running* stack (``make up``, then ``make race``).

Fires bursts of truly concurrent HTTP requests and checks the invariants the brief asks for:

1. N concurrent creates with the same slug -> exactly one 201, the rest tenant_already_exists.
2. N concurrent PATCHes with the same version -> exactly one 202, the rest tenant_version_conflict.
3. The tenant then finishes its update via the real worker and ends active with version 4.

Exits non-zero if any invariant is violated.
"""

import argparse
import asyncio
import sys
import uuid
from collections import Counter

import httpx


def summarize(responses: list[httpx.Response]) -> Counter[str]:
    out: Counter[str] = Counter()
    for r in responses:
        code = r.json().get("error", {}).get("code") if r.status_code >= 400 else None
        out[f"{r.status_code} {code or ''}".strip()] += 1
    return out


async def wait_for(client: httpx.AsyncClient, tenant_id: str, status: str) -> dict:
    try:
        async with asyncio.timeout(30):
            while True:
                tenant = (await client.get(f"/v1/tenants/{tenant_id}")).json()
                if tenant["status"] == status:
                    return tenant
                await asyncio.sleep(0.2)
    except TimeoutError:
        raise SystemExit(f"FAIL: tenant {tenant_id} did not reach {status!r} in 30s") from None


def check(label: str, ok: bool, detail: object) -> bool:
    print(f"[{'PASS' if ok else 'FAIL'}] {label}: {detail}")
    return ok


async def main(base_url: str, n: int) -> int:
    limits = httpx.Limits(max_connections=n, max_keepalive_connections=n)
    async with httpx.AsyncClient(base_url=base_url, limits=limits, timeout=30) as client:
        slug = f"race-{uuid.uuid4().hex[:8]}"
        print(f"{n} concurrent POST /v1/tenants slug={slug}")
        creates = await asyncio.gather(
            *(client.post("/v1/tenants", json={"slug": slug, "name": f"n{i}"}) for i in range(n))
        )
        created = summarize(creates)
        ok = check(
            "creates",
            created == {"201": 1, "409 tenant_already_exists": n - 1},
            dict(created),
        )

        winner = next(r.json() for r in creates if r.status_code == 201)
        tenant_id = winner["tenant"]["id"]
        tenant = await wait_for(client, tenant_id, "active")
        print(f"tenant {tenant_id} is active at version {tenant['version']}")

        print(f"{n} concurrent PATCH /v1/tenants/{tenant_id} version={tenant['version']}")
        patches = await asyncio.gather(
            *(
                client.patch(
                    f"/v1/tenants/{tenant_id}",
                    json={"name": f"patch-{i}", "version": tenant["version"]},
                )
                for i in range(n)
            )
        )
        patched = summarize(patches)
        ok &= check(
            "patches",
            patched == {"202": 1, "409 tenant_version_conflict": n - 1},
            dict(patched),
        )

        final = await wait_for(client, tenant_id, "active")
        tasks = (await client.get(f"/v1/tenants/{tenant_id}/tasks")).json()["items"]
        ok &= check("final version", final["version"] == 4, final["version"])
        ok &= check(
            "tasks",
            sorted((t["type"], t["status"]) for t in tasks)
            == [("deploy", "done"), ("update", "done")],
            [(t["type"], t["status"]) for t in tasks],
        )
    print("ALL INVARIANTS HELD" if ok else "INVARIANT VIOLATED")
    return 0 if ok else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("-n", type=int, default=50, help="concurrent requests per burst")
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.base_url, args.n)))
