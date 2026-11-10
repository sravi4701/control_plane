import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session, parse_id
from app.api.errors import error_responses
from app.api.schemas import TaskOut, TaskPage
from app.domain.errors import TaskNotFoundError
from app.domain.state_machine import TaskStatus, TaskType
from app.services import tenant_service as svc

router = APIRouter(prefix="/v1/tasks", tags=["tasks"])

Session = Annotated[AsyncSession, Depends(get_session)]


@router.get("", responses=error_responses(422))
async def list_tasks(
    session: Session,
    tenant_id: uuid.UUID | None = None,
    type: TaskType | None = None,
    status: TaskStatus | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: str | None = None,
) -> TaskPage:
    page = await svc.list_tasks(
        session, tenant_id=tenant_id, task_type=type, status=status, limit=limit, cursor=cursor
    )
    return TaskPage(items=[TaskOut.from_model(t) for t in page.items], next_cursor=page.next_cursor)


@router.get("/{task_id}", responses=error_responses(404))
async def get_task(task_id: str, session: Session) -> TaskOut:
    tid = parse_id(task_id, TaskNotFoundError, "task_id")
    return TaskOut.from_model(await svc.get_task(session, tid))
