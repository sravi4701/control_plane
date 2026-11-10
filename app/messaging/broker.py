"""Broker connection and publishing helpers."""

import json
import logging
from typing import Any, Protocol

import aio_pika
from aio_pika.abc import AbstractExchange, AbstractRobustConnection
from aiormq.exceptions import AMQPConnectionError
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

log = logging.getLogger(__name__)


@retry(
    retry=retry_if_exception_type((AMQPConnectionError, ConnectionError, OSError)),
    wait=wait_exponential(multiplier=0.5, max=10),
    stop=stop_after_attempt(30),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
async def connect(url: str) -> AbstractRobustConnection:
    """Connect with exponential backoff at startup.

    Once connected, ``connect_robust`` transparently reconnects and restores channels,
    exchanges, queues and consumers after a broker outage.
    """
    return await aio_pika.connect_robust(url)


class EventPublisher(Protocol):
    async def publish(self, routing_key: str, payload: dict[str, Any], message_id: str) -> None: ...


class AmqpPublisher:
    """Publishes persistent JSON messages and waits for the broker's publisher confirm.

    The channel must be opened with ``publisher_confirms=True`` (aio-pika's default) so that
    ``publish`` returns only once the broker has taken responsibility for the message, and
    ``on_return_raises=True`` so an unroutable message is an error instead of silently dropped.
    """

    def __init__(self, exchange: AbstractExchange) -> None:
        self._exchange = exchange

    async def publish(self, routing_key: str, payload: dict[str, Any], message_id: str) -> None:
        message = aio_pika.Message(
            body=json.dumps(payload).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=message_id,
        )
        await self._exchange.publish(message, routing_key=routing_key, mandatory=True)
