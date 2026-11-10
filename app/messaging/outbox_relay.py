"""Outbox relay: moves committed outbox rows onto the broker.

Guarantees
* Publish-after-commit: the relay can only see committed rows, so an event is never published
  for a transaction that rolled back or has not committed yet.
* At-least-once: a row is marked published only after the broker confirms it. If the process
  dies between the confirm and the commit, the row is re-published on restart. Consumers
  deduplicate on the task id (which is also the AMQP ``message_id``).
* Safe to run in several replicas: ``FOR UPDATE SKIP LOCKED`` hands each row to one relay.
  Per-tenant ordering is not at risk because a tenant never has more than one open task.
"""

import asyncio
import contextlib
import logging
from typing import Any

import asyncpg
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import OutboxEvent
from app.messaging.broker import EventPublisher
from app.messaging.outbox import OUTBOX_NOTIFY_CHANNEL
from app.timeutil import utcnow

log = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        publisher: EventPublisher,
        *,
        batch_size: int = 100,
        poll_interval_s: float = 1.0,
        max_backoff_s: float = 30.0,
        listen_database_url: str | None = None,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval_s = poll_interval_s
        self._max_backoff_s = max_backoff_s
        self._listen_database_url = listen_database_url
        self._wakeup = asyncio.Event()
        self._listener: Any = None

    async def publish_pending(self) -> int:
        """Publish one batch of unpublished rows. Returns how many were published.

        The whole batch is one transaction: if a publish fails, the rows already published in
        this batch are rolled back to "unpublished" and sent again next time (a duplicate,
        which at-least-once consumers must tolerate anyway).
        """
        async with self._sessionmaker() as session, session.begin():
            rows = (
                await session.scalars(
                    select(OutboxEvent)
                    .where(OutboxEvent.published_at.is_(None))
                    .order_by(OutboxEvent.id)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            for row in rows:
                await self._publisher.publish(row.routing_key, row.payload, str(row.task_id))
                row.published_at = utcnow()
        if rows:
            log.info("outbox relay published %d event(s)", len(rows))
        return len(rows)

    async def run(self) -> None:
        await self._start_listener()
        failures = 0
        try:
            while True:
                # Clear *before* querying: a NOTIFY arriving mid-batch then wakes the next wait
                # instead of being lost.
                self._wakeup.clear()
                try:
                    published = await self.publish_pending()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Broker or database unavailable: back off exponentially (capped). Rows stay
                    # in the outbox, so nothing is lost while we wait.
                    failures += 1
                    delay = self.backoff_delay(failures)
                    log.warning(
                        "outbox relay batch failed (attempt %d); retrying in %.1fs",
                        failures,
                        delay,
                        exc_info=failures == 1,
                    )
                    await asyncio.sleep(delay)
                    continue
                failures = 0
                if published >= self._batch_size:
                    continue  # likely more waiting; don't sleep
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._wakeup.wait(), timeout=self._poll_interval_s)
        finally:
            await self._stop_listener()

    def backoff_delay(self, failures: int) -> float:
        return float(min(0.5 * 2 ** (failures - 1), self._max_backoff_s))

    async def _start_listener(self) -> None:
        """Best-effort LISTEN for low latency. Polling still works if this fails."""
        if self._listen_database_url is None:
            return
        dsn = make_url(self._listen_database_url).set(drivername="postgresql")
        try:
            self._listener = await asyncpg.connect(dsn.render_as_string(hide_password=False))
            await self._listener.add_listener(OUTBOX_NOTIFY_CHANNEL, self._on_notify)
        except Exception:
            log.warning("outbox LISTEN unavailable; falling back to polling", exc_info=True)
            self._listener = None

    async def _stop_listener(self) -> None:
        if self._listener is not None:
            with contextlib.suppress(Exception):
                await self._listener.close()
            self._listener = None

    def _on_notify(self, *_args: object) -> None:
        self._wakeup.set()
