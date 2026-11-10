"""Application entry point: HTTP API plus the outbox relay and progress consumer.

All three run in one process so the service is a single deployable unit. The relay and consumer
are started from the FastAPI lifespan and stopped cleanly on shutdown.
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text

from app.api import tasks, tenants
from app.api.errors import install_error_handlers
from app.config import Settings, get_settings
from app.db.session import create_engine, create_sessionmaker
from app.messaging.broker import AmqpPublisher, connect
from app.messaging.outbox_relay import OutboxRelay
from app.messaging.progress_consumer import ProgressConsumer
from app.messaging.topology import declare_topology

log = logging.getLogger(__name__)


@asynccontextmanager
async def _background_workers(app: FastAPI, settings: Settings) -> AsyncIterator[None]:
    connection = await connect(settings.amqp_url)
    try:
        # Separate channels: the relay's publisher confirms and the consumer's prefetch don't
        # interfere, and a channel-level error on one side doesn't take down the other.
        publish_channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        topology = await declare_topology(publish_channel)

        consume_channel = await connection.channel()
        await consume_channel.set_qos(prefetch_count=settings.consumer_prefetch)
        updates_queue = await consume_channel.get_queue(topology.updates_queue.name)

        relay = OutboxRelay(
            app.state.sessionmaker,
            AmqpPublisher(topology.tasks_exchange),
            batch_size=settings.outbox_batch_size,
            poll_interval_s=settings.outbox_poll_interval_s,
            listen_database_url=settings.database_url,
        )
        consumer = ProgressConsumer(
            app.state.sessionmaker, updates_queue, max_attempts=settings.consumer_max_attempts
        )
        await consumer.start()
        relay_task = asyncio.create_task(relay.run(), name="outbox-relay")
        try:
            yield
        finally:
            await consumer.stop()
            relay_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await relay_task
    finally:
        await connection.close()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    engine = create_engine(settings)
    app.state.sessionmaker = create_sessionmaker(engine)
    try:
        if settings.run_background_workers:
            async with _background_workers(app, settings):
                yield
        else:
            yield
    finally:
        await engine.dispose()


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    logging.basicConfig(
        level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    app = FastAPI(
        title="Tenant Provisioning Control Plane",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.settings = settings
    install_error_handlers(app)
    app.include_router(tenants.router)
    app.include_router(tasks.router)

    @app.get("/healthz", tags=["ops"])
    async def healthz() -> dict[str, str]:
        async with app.state.sessionmaker() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ok"}

    return app


app = create_app()
