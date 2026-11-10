"""SQLAlchemy models.

Statuses are stored as text with CHECK constraints (rather than native Postgres enums) so adding
a status later is a simple constraint swap instead of an ``ALTER TYPE`` migration.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.domain.state_machine import TaskStatus, TaskType, TenantStatus


def _in(column: str, values: type[TenantStatus] | type[TaskStatus] | type[TaskType]) -> str:
    joined = ", ".join(f"'{v.value}'" for v in values)
    return f"{column} IN ({joined})"


class Base(DeclarativeBase):
    pass


class Tenant(Base):
    __tablename__ = "tenants"
    __table_args__ = (
        UniqueConstraint("slug", name="uq_tenants_slug"),
        CheckConstraint(_in("status", TenantStatus), name="ck_tenants_status"),
        CheckConstraint("version >= 1", name="ck_tenants_version_positive"),
        Index("ix_tenants_created_at_id", "created_at", "id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    # Unique across *all* rows. Tenants are never hard-deleted (destroyed is a status), so a
    # destroyed tenant's slug stays reserved. The constraint, not application code, decides the
    # winner of concurrent creates with the same slug.
    slug: Mapped[str] = mapped_column(String(28), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        CheckConstraint(_in("type", TaskType), name="ck_tasks_type"),
        CheckConstraint(_in("status", TaskStatus), name="ck_tasks_status"),
        Index("ix_tasks_created_at_id", "created_at", "id"),
        Index("ix_tasks_tenant_id_created_at", "tenant_id", "created_at"),
        # Safety net for the "at most one open task per tenant" invariant. The tenant status
        # guards already guarantee it; this index makes a bug there fail loudly instead of
        # silently creating a second open task.
        Index(
            "uq_tasks_one_open_per_tenant",
            "tenant_id",
            unique=True,
            postgresql_where=text("status IN ('accepted', 'in_progress')"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    type: Mapped[str] = mapped_column(String(16), nullable=False)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class OutboxEvent(Base):
    """Transactional outbox: written in the same transaction as the task it announces.

    The relay publishes rows only after they are committed, which is what guarantees
    "publish after durable commit, never on rollback".
    """

    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_unpublished",
            "id",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    task_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tasks.id"), nullable=False
    )
    routing_key: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ProcessedMessage(Base):
    """Idempotency ledger for inbound progress updates, keyed by the update's ``message_id``."""

    __tablename__ = "processed_messages"

    message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    task_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    outcome: Mapped[str] = mapped_column(String(16), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
