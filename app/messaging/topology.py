"""Broker topology, shared by the control plane and the worker simulator.

Both sides declare everything idempotently on startup, so whichever process starts first creates
the exchanges and queues and no message is lost because "the other side wasn't up yet".

    control plane --(tasks, task.<type>)--> worker.tasks --> worker
    worker --(task-updates, task-update.<status>)--> control-plane.task-updates --> control plane
                                                        | rejected / delivery limit exceeded
                                                        v
                                     task-updates.dlx --> control-plane.task-updates.dlq
"""

from dataclasses import dataclass
from typing import Any

from aio_pika import ExchangeType
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractQueue

TASKS_EXCHANGE = "tasks"
TASKS_QUEUE = "worker.tasks"
TASKS_DLX = "tasks.dlx"
TASKS_DLQ = "worker.tasks.dlq"

UPDATES_EXCHANGE = "task-updates"
UPDATES_QUEUE = "control-plane.task-updates"
UPDATES_DLX = "task-updates.dlx"
UPDATES_DLQ = "control-plane.task-updates.dlq"

# Quorum queues count redeliveries; after this many a message is dead-lettered instead of being
# redelivered forever. This bounds the damage of a message that keeps crashing its consumer.
DELIVERY_LIMIT = 10


def task_routing_key(task_type: str) -> str:
    return f"task.{task_type}"


def update_routing_key(status: str) -> str:
    return f"task-update.{status}"


def _quorum_args(dlx: str) -> dict[str, Any]:
    return {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": dlx,
        "x-delivery-limit": DELIVERY_LIMIT,
    }


@dataclass(frozen=True)
class Topology:
    tasks_exchange: AbstractExchange
    tasks_queue: AbstractQueue
    tasks_dlq: AbstractQueue
    updates_exchange: AbstractExchange
    updates_queue: AbstractQueue
    updates_dlq: AbstractQueue


async def declare_topology(channel: AbstractChannel) -> Topology:
    tasks_exchange = await channel.declare_exchange(
        TASKS_EXCHANGE, ExchangeType.TOPIC, durable=True
    )
    tasks_dlx = await channel.declare_exchange(TASKS_DLX, ExchangeType.FANOUT, durable=True)
    tasks_queue = await channel.declare_queue(
        TASKS_QUEUE, durable=True, arguments=_quorum_args(TASKS_DLX)
    )
    await tasks_queue.bind(tasks_exchange, routing_key="task.*")
    tasks_dlq = await channel.declare_queue(
        TASKS_DLQ, durable=True, arguments={"x-queue-type": "quorum"}
    )
    await tasks_dlq.bind(tasks_dlx)

    updates_exchange = await channel.declare_exchange(
        UPDATES_EXCHANGE, ExchangeType.TOPIC, durable=True
    )
    updates_dlx = await channel.declare_exchange(UPDATES_DLX, ExchangeType.FANOUT, durable=True)
    updates_queue = await channel.declare_queue(
        UPDATES_QUEUE, durable=True, arguments=_quorum_args(UPDATES_DLX)
    )
    await updates_queue.bind(updates_exchange, routing_key="task-update.*")
    updates_dlq = await channel.declare_queue(
        UPDATES_DLQ, durable=True, arguments={"x-queue-type": "quorum"}
    )
    await updates_dlq.bind(updates_dlx)

    return Topology(
        tasks_exchange=tasks_exchange,
        tasks_queue=tasks_queue,
        tasks_dlq=tasks_dlq,
        updates_exchange=updates_exchange,
        updates_queue=updates_queue,
        updates_dlq=updates_dlq,
    )
