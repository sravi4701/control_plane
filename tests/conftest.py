"""Shared fixtures.

Integration tests run against real Postgres and RabbitMQ (the compose services), isolated from
the running stack: a dedicated database (``control_plane_test``) and a dedicated vhost
(``cp-test``) that are recreated at the start of every session.
"""

import asyncio
import os
import urllib.parse
from collections.abc import AsyncIterator, Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import aio_pika
import asyncpg
import httpx
import pytest
from aio_pika.abc import AbstractChannel, AbstractRobustConnection
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings
from app.db.session import create_engine, create_sessionmaker
from app.main import create_app
from app.messaging.topology import Topology, declare_topology

DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://control_plane:control_plane@localhost:5432/control_plane_test",
)
AMQP_URL = os.environ.get(
    "TEST_AMQP_URL", "amqp://control_plane:control_plane@localhost:5672/cp-test"
)
RABBITMQ_MGMT_URL = os.environ.get("TEST_RABBITMQ_MGMT_URL", "http://localhost:15672")
RABBITMQ_USER = os.environ.get("TEST_RABBITMQ_USER", "control_plane")
RABBITMQ_PASSWORD = os.environ.get("TEST_RABBITMQ_PASSWORD", "control_plane")


def _run_in_thread(fn: Callable[[], Any]) -> Any:
    # Alembic's env.py calls asyncio.run(); running it in a separate thread keeps it away from
    # pytest-asyncio's event loop.
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(fn).result()


async def _recreate_database(url: str) -> None:
    target = make_url(url)
    admin = target.set(drivername="postgresql", database="postgres")
    conn = await asyncpg.connect(admin.render_as_string(hide_password=False))
    try:
        await conn.execute(f'DROP DATABASE IF EXISTS "{target.database}" WITH (FORCE)')
        await conn.execute(f'CREATE DATABASE "{target.database}"')
    finally:
        await conn.close()


def _migrate(url: str) -> None:
    cfg = Config("alembic.ini")
    cfg.attributes["database_url"] = url
    cfg.attributes["configure_logger"] = False
    command.upgrade(cfg, "head")
    # Exercise the downgrade path too, then land on head for the tests.
    command.downgrade(cfg, "base")
    command.upgrade(cfg, "head")


@pytest.fixture(scope="session")
def database_url() -> str:
    _run_in_thread(lambda: asyncio.run(_recreate_database(DATABASE_URL)))
    _run_in_thread(lambda: _migrate(DATABASE_URL))
    return DATABASE_URL


@pytest.fixture(scope="session")
def settings(database_url: str) -> Settings:
    return Settings(
        database_url=database_url,
        amqp_url=AMQP_URL,
        db_pool_size=40,
        db_max_overflow=10,
        outbox_poll_interval_s=0.2,
        run_background_workers=False,
        log_level="WARNING",
    )


@pytest.fixture(scope="session")
async def engine(settings: Settings) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(settings)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_sessionmaker(engine)


async def truncate_all(sm: async_sessionmaker[AsyncSession]) -> None:
    async with sm() as session, session.begin():
        await session.execute(
            text("TRUNCATE tenants, tasks, outbox, processed_messages RESTART IDENTITY CASCADE")
        )


@pytest.fixture
async def client(
    settings: Settings, sessionmaker: async_sessionmaker[AsyncSession]
) -> AsyncIterator[httpx.AsyncClient]:
    """HTTP client against the app, without background workers (tests drive them directly)."""
    app = create_app(settings)
    app.state.sessionmaker = sessionmaker
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# --- RabbitMQ -------------------------------------------------------------------------------


def _recreate_vhost() -> None:
    vhost = urllib.parse.urlparse(AMQP_URL).path.lstrip("/")
    quoted = urllib.parse.quote(vhost, safe="")
    auth = (RABBITMQ_USER, RABBITMQ_PASSWORD)
    with httpx.Client(base_url=RABBITMQ_MGMT_URL, auth=auth, timeout=10) as http:
        http.delete(f"/api/vhosts/{quoted}")  # 404 if absent; fine
        http.put(f"/api/vhosts/{quoted}").raise_for_status()
        http.put(
            f"/api/permissions/{quoted}/{RABBITMQ_USER}",
            json={"configure": ".*", "write": ".*", "read": ".*"},
        ).raise_for_status()


@pytest.fixture(scope="session")
async def amqp_connection() -> AsyncIterator[AbstractRobustConnection]:
    _recreate_vhost()
    connection = await aio_pika.connect_robust(AMQP_URL)
    yield connection
    await connection.close()


@pytest.fixture
async def channel(amqp_connection: AbstractRobustConnection) -> AsyncIterator[AbstractChannel]:
    ch = await amqp_connection.channel(publisher_confirms=True, on_return_raises=True)
    yield ch
    await ch.close()


@pytest.fixture
async def topology(channel: AbstractChannel) -> Topology:
    """Declared topology with every queue empty."""
    topo = await declare_topology(channel)
    for queue in (
        topo.tasks_queue,
        topo.tasks_dlq,
        topo.updates_queue,
        topo.updates_dlq,
    ):
        await queue.purge()
    return topo


async def eventually[T](
    probe: Callable[[], Awaitable[T | None]], *, timeout: float = 10.0, interval: float = 0.05
) -> T:
    """Poll ``probe`` until it returns something truthy, or fail after ``timeout`` seconds."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        result = await probe()
        if result:
            return result
        if loop.time() > deadline:
            raise AssertionError(f"condition not met within {timeout}s")
        await asyncio.sleep(interval)
