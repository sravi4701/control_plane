"""Worker simulator: consumes tasks and reports simulated progress back to the control plane.

For each task it publishes ``in_progress`` and then ``done`` or ``failed``, with configurable
delays and failure rate. Chaos switches reproduce the delivery anomalies the control plane must
tolerate:

* ``duplicate_rate``: publish some updates twice (same ``message_id``).
* ``reorder``: publish the terminal update *before* ``in_progress``.
"""

import asyncio
import json
import logging
import random
from dataclasses import dataclass

import aio_pika
from aio_pika.abc import AbstractExchange, AbstractIncomingMessage
from pydantic import ValidationError

from app.messaging.schemas import TaskEvent, TaskProgressUpdate, progress_message_id
from app.messaging.topology import update_routing_key
from app.timeutil import utcnow

log = logging.getLogger("worker")


@dataclass(frozen=True)
class WorkerConfig:
    fail_rate: float = 0.0
    min_delay_ms: int = 500
    max_delay_ms: int = 2000
    duplicate_rate: float = 0.0
    reorder: bool = False

    def __post_init__(self) -> None:
        for name in ("fail_rate", "duplicate_rate"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1, got {value}")
        if self.min_delay_ms < 0 or self.max_delay_ms < self.min_delay_ms:
            raise ValueError("delays must satisfy 0 <= min_delay_ms <= max_delay_ms")


class WorkerSimulator:
    def __init__(
        self, config: WorkerConfig, updates_exchange: AbstractExchange, rng: random.Random
    ) -> None:
        self._config = config
        self._exchange = updates_exchange
        # Not used for anything security-related; it only drives simulated delays and outcomes.
        self._rng = rng
        # Best-effort dedupe of task redeliveries within this process. It is not needed for
        # correctness: update ids are deterministic, so reprocessing a task after a restart
        # re-sends the same ids and the control plane drops them as duplicates.
        self._completed: set[str] = set()

    async def handle(self, message: AbstractIncomingMessage) -> None:
        try:
            event = TaskEvent.model_validate_json(message.body)
        except ValidationError:
            log.warning("dropping malformed task message %s", message.message_id)
            await message.reject(requeue=False)
            return

        if str(event.id) in self._completed:
            log.info("task %s already processed; acking redelivery", event.id)
            await message.ack()
            return

        try:
            await self.process(event)
        except Exception:
            log.exception("failed to process task %s; requeueing", event.id)
            await message.nack(requeue=True)
            return
        self._completed.add(str(event.id))
        # Ack only after the terminal update is confirmed by the broker: if we crash before
        # this, the task is redelivered and processed again (at-least-once).
        await message.ack()

    async def process(self, event: TaskEvent) -> None:
        log.info("task %s (%s) for tenant %s: started", event.id, event.type, event.tenant_id)
        failed = self._rng.random() < self._config.fail_rate
        terminal = self._update(event, "failed" if failed else "done")
        in_progress = self._update(event, "in_progress")

        await self._sleep()
        if self._config.reorder:
            await self._publish(terminal)
            await self._publish(in_progress)
        else:
            await self._publish(in_progress)
            await self._sleep()
            await self._publish(terminal)
        log.info("task %s: %s", event.id, terminal.status)

    def _update(self, event: TaskEvent, status: str) -> TaskProgressUpdate:
        return TaskProgressUpdate.model_validate(
            {
                "message_id": progress_message_id(event.id, status),
                "task_id": event.id,
                "status": status,
                "error": "simulated provisioning failure" if status == "failed" else None,
                "occurred_at": utcnow(),
            }
        )

    async def _publish(self, update: TaskProgressUpdate) -> None:
        copies = 2 if self._rng.random() < self._config.duplicate_rate else 1
        body = json.dumps(update.model_dump(mode="json")).encode()
        for _ in range(copies):
            await self._exchange.publish(
                aio_pika.Message(
                    body=body,
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    message_id=str(update.message_id),
                ),
                routing_key=update_routing_key(update.status),
                mandatory=True,
            )
        if copies > 1:
            log.info("published %s for task %s twice (duplicate)", update.status, update.task_id)

    async def _sleep(self) -> None:
        delay_ms = self._rng.uniform(self._config.min_delay_ms, self._config.max_delay_ms)
        await asyncio.sleep(delay_ms / 1000)
