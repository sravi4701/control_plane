"""Tenant and task state machines.

This module is pure (no I/O) so every rule can be unit-tested exhaustively. The service layer
turns these rules into *conditional* SQL updates so the guards are enforced atomically by the
database rather than by a read-then-write in Python.
"""

from enum import StrEnum


class TenantStatus(StrEnum):
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    UPDATING = "updating"
    DESTROYING = "destroying"
    DESTROYED = "destroyed"
    FAILED = "failed"


class TaskType(StrEnum):
    DEPLOY = "deploy"
    UPDATE = "update"
    DESTROY = "destroy"


class TaskStatus(StrEnum):
    ACCEPTED = "accepted"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"


TERMINAL_TASK_STATUSES: frozenset[TaskStatus] = frozenset({TaskStatus.DONE, TaskStatus.FAILED})
OPEN_TASK_STATUSES: frozenset[TaskStatus] = frozenset({TaskStatus.ACCEPTED, TaskStatus.IN_PROGRESS})

# Task progress is monotonic: a status may only be replaced by one with a strictly higher rank.
_TASK_RANK: dict[TaskStatus, int] = {
    TaskStatus.ACCEPTED: 0,
    TaskStatus.IN_PROGRESS: 1,
    TaskStatus.DONE: 2,
    TaskStatus.FAILED: 2,
}

# API-driven tenant transitions: which tenant statuses allow the operation.
UPDATABLE_FROM: frozenset[TenantStatus] = frozenset({TenantStatus.ACTIVE})
DELETABLE_FROM: frozenset[TenantStatus] = frozenset({TenantStatus.ACTIVE, TenantStatus.FAILED})

# While a task of a given type is open, the tenant sits in this status.
IN_FLIGHT_STATUS: dict[TaskType, TenantStatus] = {
    TaskType.DEPLOY: TenantStatus.PROVISIONING,
    TaskType.UPDATE: TenantStatus.UPDATING,
    TaskType.DESTROY: TenantStatus.DESTROYING,
}

_SUCCESS_STATUS: dict[TaskType, TenantStatus] = {
    TaskType.DEPLOY: TenantStatus.ACTIVE,
    TaskType.UPDATE: TenantStatus.ACTIVE,
    TaskType.DESTROY: TenantStatus.DESTROYED,
}


def is_terminal(status: TaskStatus) -> bool:
    return status in TERMINAL_TASK_STATUSES


def can_advance_task(current: TaskStatus, new: TaskStatus) -> bool:
    """Whether a progress update moving a task from ``current`` to ``new`` should be applied.

    * Anything arriving for a terminal task is stale (``done``/``failed`` are final).
    * Only strictly forward moves apply, so a late ``in_progress`` never regresses a task.
    * Forward *jumps* are allowed (``accepted -> done``): with at-least-once delivery and no
      ordering guarantee the ``in_progress`` update may arrive late or not at all, and the
      terminal outcome is the authoritative fact.
    """
    if is_terminal(current):
        return False
    return _TASK_RANK[new] > _TASK_RANK[current]


def tenant_status_after_task(task_type: TaskType, task_status: TaskStatus) -> TenantStatus | None:
    """The tenant status implied by a task outcome, or ``None`` if the tenant should not move."""
    if task_status == TaskStatus.DONE:
        return _SUCCESS_STATUS[task_type]
    if task_status == TaskStatus.FAILED:
        return TenantStatus.FAILED
    return None
