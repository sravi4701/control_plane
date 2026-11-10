"""Consumes worker progress updates from the broker.

Failure handling, in order of preference:
* Valid message, applied or ignored as duplicate/stale -> ack.
* Malformed message, or a task that doesn't exist (poison) -> reject without requeue; RabbitMQ
  dead-letters it to ``control-plane.task-updates.dlq`` for inspection. The consumer keeps going.
* Transient failure (database unreachable, connection dropped) -> retried in-process with
  exponential backoff. If it still fails, nack with requeue so the broker redelivers it.
* Unexpected error -> nack with requeue. The quorum queue's delivery limit dead-letters a message
  that keeps failing, so one bad message cannot loop forever or block the queue.

Nothing escapes ``handle``: a bad message can never kill the consumer.
"""

import logging

from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from pydantic import ValidationError
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt, wait_exponential

from app.messaging.schemas import TaskProgressUpdate
from app.services.progress_service import ApplyOutcome, UnknownTaskError, apply_progress_update

log = logging.getLogger(__name__)


def is_transient(exc: BaseException) -> bool:
    if isinstance(exc, OperationalError | InterfaceError | ConnectionError | TimeoutError):
        return True
    if isinstance(exc, DBAPIError) and exc.connection_invalidated:
        return True
    # asyncpg raises plain OSError (e.g. connection refused) while establishing a connection.
    return isinstance(exc, OSError)


class ProgressConsumer:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        queue: AbstractQueue,
        *,
        max_attempts: int = 5,
        backoff_multiplier_s: float = 0.2,
        backoff_max_s: float = 5.0,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._queue = queue
        self._max_attempts = max_attempts
        self._backoff_multiplier_s = backoff_multiplier_s
        self._backoff_max_s = backoff_max_s
        self._consumer_tag: str | None = None

    async def start(self) -> None:
        self._consumer_tag = await self._queue.consume(self.handle)
        log.info("progress consumer started on %s", self._queue.name)

    async def stop(self) -> None:
        if self._consumer_tag is not None:
            await self._queue.cancel(self._consumer_tag)
            self._consumer_tag = None

    async def handle(self, message: AbstractIncomingMessage) -> None:
        try:
            update = TaskProgressUpdate.model_validate_json(message.body)
        except ValidationError as exc:
            log.warning(
                "poison message %s rejected to DLQ: invalid payload: %s",
                message.message_id,
                exc.errors(include_url=False),
            )
            await message.reject(requeue=False)
            return

        try:
            outcome = await self._apply_with_retry(update)
        except UnknownTaskError as exc:
            log.warning("poison message %s rejected to DLQ: %s", update.message_id, exc)
            await message.reject(requeue=False)
            return
        except Exception:
            log.exception(
                "failed to apply update %s for task %s; requeueing",
                update.message_id,
                update.task_id,
            )
            await message.nack(requeue=True)
            return

        log.info(
            "update %s (task %s -> %s): %s",
            update.message_id,
            update.task_id,
            update.status,
            outcome.value,
        )
        await message.ack()

    async def _apply_with_retry(self, update: TaskProgressUpdate) -> ApplyOutcome:
        retrying = AsyncRetrying(
            retry=retry_if_exception(is_transient),
            stop=stop_after_attempt(self._max_attempts),
            wait=wait_exponential(multiplier=self._backoff_multiplier_s, max=self._backoff_max_s),
            reraise=True,
        )
        async for attempt in retrying:
            with attempt:
                async with self._sessionmaker() as session:
                    return await apply_progress_update(session, update)
        raise AssertionError("unreachable: tenacity re-raises after the last attempt")
