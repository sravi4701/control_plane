import uuid
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from app.api.schemas import TenantCreate, TenantUpdate
from app.domain.errors import InvalidCursorError
from app.messaging.schemas import TaskProgressUpdate, progress_message_id
from app.services.pagination import Cursor
from app.timeutil import isoformat_ms, utcnow


@pytest.mark.parametrize(
    "slug",
    ["abc", "a-b", "a1b", "acme-corp", "a" * 28, "a" + "-" * 26 + "b", "team-42"],
)
def test_valid_slugs(slug: str) -> None:
    assert TenantCreate(slug=slug, name="x").slug == slug


@pytest.mark.parametrize(
    "slug",
    [
        "ab",  # too short
        "a" * 29,  # too long
        "1abc",  # must start with a letter
        "-abc",
        "abc-",  # must end with a letter or digit
        "Abc",  # lowercase only
        "ab_c",
        "ab.c",
        "abc\n",
        "",
    ],
)
def test_invalid_slugs(slug: str) -> None:
    with pytest.raises(ValidationError):
        TenantCreate(slug=slug, name="x")


@pytest.mark.parametrize("name", ["", "   "])
def test_name_must_be_non_empty(name: str) -> None:
    with pytest.raises(ValidationError):
        TenantCreate(slug="abc", name=name)


def test_create_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        TenantCreate.model_validate({"slug": "abc", "name": "x", "status": "active"})


def test_update_requires_version_and_a_field() -> None:
    with pytest.raises(ValidationError):
        TenantUpdate.model_validate({"name": "x"})
    with pytest.raises(ValidationError):
        TenantUpdate.model_validate({"version": 1})
    with pytest.raises(ValidationError):
        TenantUpdate.model_validate({"name": "x", "version": 0})
    assert TenantUpdate.model_validate({"name": " x ", "version": 3}).name == "x"


def test_isoformat_ms() -> None:
    dt = datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=UTC)
    assert isoformat_ms(dt) == "2026-01-02T03:04:05.678Z"
    other_tz = dt.astimezone(timezone(timedelta(hours=5, minutes=30)))
    assert isoformat_ms(other_tz) == "2026-01-02T03:04:05.678Z"
    with pytest.raises(ValueError, match="naive"):
        isoformat_ms(datetime(2026, 1, 1))


def test_utcnow_is_millisecond_precision() -> None:
    assert utcnow().microsecond % 1000 == 0
    assert utcnow().tzinfo is UTC


def test_cursor_round_trip() -> None:
    c = Cursor(created_at=utcnow(), id=uuid.uuid4())
    assert Cursor.decode(c.encode()) == c


@pytest.mark.parametrize("token", ["", "garbage", "e30", "eyJ0IjogIngiLCAiaWQiOiAiMSJ9"])
def test_malformed_cursor(token: str) -> None:
    with pytest.raises(InvalidCursorError):
        Cursor.decode(token)


def test_progress_update_schema() -> None:
    task_id = uuid.uuid4()
    msg = TaskProgressUpdate.model_validate(
        {
            "message_id": str(uuid.uuid4()),
            "task_id": str(task_id),
            "status": "done",
            "occurred_at": "2026-01-01T00:00:00.000Z",
            "some_future_field": 1,  # ignored: forward compatible
        }
    )
    assert msg.task_id == task_id
    for bad in (
        {"status": "accepted"},  # the worker can't report accepted
        {"status": "bogus"},
        {"message_id": "not-a-uuid"},
        {"occurred_at": "2026-01-01T00:00:00"},  # timezone required
    ):
        payload = {
            "message_id": str(uuid.uuid4()),
            "task_id": str(task_id),
            "status": "done",
            "occurred_at": "2026-01-01T00:00:00Z",
            **bad,
        }
        with pytest.raises(ValidationError):
            TaskProgressUpdate.model_validate(payload)


def test_progress_message_id_is_deterministic() -> None:
    task_id = uuid.uuid4()
    assert progress_message_id(task_id, "done") == progress_message_id(task_id, "done")
    assert progress_message_id(task_id, "done") != progress_message_id(task_id, "in_progress")
    assert progress_message_id(task_id, "done") != progress_message_id(uuid.uuid4(), "done")
