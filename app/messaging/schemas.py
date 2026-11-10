"""Wire formats for both directions. Shared by the control plane and the worker simulator."""

import uuid
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict

from app.domain.state_machine import TaskStatus, TaskType


class TaskEvent(BaseModel):
    """Outbound: control plane -> worker. The task *is* the event; ``id`` is the task id.

    The same id is set as the AMQP ``message_id``, so consumers can deduplicate redeliveries.
    """

    model_config = ConfigDict(extra="ignore")

    id: uuid.UUID
    type: TaskType
    tenant_id: uuid.UUID
    status: TaskStatus
    created_at: str


ProgressStatus = Literal["in_progress", "done", "failed"]


class TaskProgressUpdate(BaseModel):
    """Inbound: worker -> control plane.

    ``message_id`` is the idempotency key: the control plane applies each id at most once.
    The worker derives it deterministically from ``(task_id, status)``, so even a worker that
    restarts and reprocesses a task re-sends the *same* ids.
    """

    # Unknown fields are ignored (forward compatible); missing or mistyped ones are poison.
    model_config = ConfigDict(extra="ignore")

    message_id: uuid.UUID
    task_id: uuid.UUID
    status: ProgressStatus
    error: str | None = None
    occurred_at: AwareDatetime

    @property
    def task_status(self) -> TaskStatus:
        return TaskStatus(self.status)


def progress_message_id(task_id: uuid.UUID, status: str) -> uuid.UUID:
    """Deterministic idempotency key for a (task, status) progress update."""
    return uuid.uuid5(task_id, f"progress:{status}")
