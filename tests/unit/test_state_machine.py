import pytest

from app.domain.state_machine import (
    DELETABLE_FROM,
    IN_FLIGHT_STATUS,
    UPDATABLE_FROM,
    TaskStatus,
    TaskType,
    TenantStatus,
    can_advance_task,
    is_terminal,
    tenant_status_after_task,
)

A, P, D, F = TaskStatus.ACCEPTED, TaskStatus.IN_PROGRESS, TaskStatus.DONE, TaskStatus.FAILED

# Every (current, new) pair, and whether it may be applied.
TASK_TRANSITIONS = {
    (A, A): False,
    (A, P): True,
    (A, D): True,  # forward jump: in_progress lost or reordered
    (A, F): True,
    (P, A): False,
    (P, P): False,
    (P, D): True,
    (P, F): True,
    (D, A): False,
    (D, P): False,  # stale in_progress after done must not regress
    (D, D): False,
    (D, F): False,  # terminal is final
    (F, A): False,
    (F, P): False,
    (F, D): False,
    (F, F): False,
}


def test_transition_table_is_exhaustive() -> None:
    assert set(TASK_TRANSITIONS) == {(a, b) for a in TaskStatus for b in TaskStatus}


@pytest.mark.parametrize(
    ("current", "new", "allowed"), [(*k, v) for k, v in TASK_TRANSITIONS.items()]
)
def test_can_advance_task(current: TaskStatus, new: TaskStatus, allowed: bool) -> None:
    assert can_advance_task(current, new) is allowed


def test_terminal_statuses() -> None:
    assert {s for s in TaskStatus if is_terminal(s)} == {D, F}


@pytest.mark.parametrize(
    ("task_type", "task_status", "expected"),
    [
        (TaskType.DEPLOY, D, TenantStatus.ACTIVE),
        (TaskType.UPDATE, D, TenantStatus.ACTIVE),
        (TaskType.DESTROY, D, TenantStatus.DESTROYED),
        (TaskType.DEPLOY, F, TenantStatus.FAILED),
        (TaskType.UPDATE, F, TenantStatus.FAILED),
        (TaskType.DESTROY, F, TenantStatus.FAILED),
        (TaskType.DEPLOY, P, None),
        (TaskType.UPDATE, A, None),
    ],
)
def test_tenant_status_after_task(
    task_type: TaskType, task_status: TaskStatus, expected: TenantStatus | None
) -> None:
    assert tenant_status_after_task(task_type, task_status) == expected


def test_api_guards_match_spec() -> None:
    assert {TenantStatus.ACTIVE} == UPDATABLE_FROM
    assert {TenantStatus.ACTIVE, TenantStatus.FAILED} == DELETABLE_FROM
    assert IN_FLIGHT_STATUS == {
        TaskType.DEPLOY: TenantStatus.PROVISIONING,
        TaskType.UPDATE: TenantStatus.UPDATING,
        TaskType.DESTROY: TenantStatus.DESTROYING,
    }


def test_tenant_failed_reachable_from_every_in_flight_status() -> None:
    # Spec: tenant `failed` is reachable from provisioning, updating and destroying.
    sources = {
        IN_FLIGHT_STATUS[t]
        for t in TaskType
        if tenant_status_after_task(t, F) == TenantStatus.FAILED
    }
    assert sources == {TenantStatus.PROVISIONING, TenantStatus.UPDATING, TenantStatus.DESTROYING}
