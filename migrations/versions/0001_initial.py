"""Initial schema: tenants, tasks, outbox, processed_messages.

Revision ID: 0001
Revises:
Create Date: 2026-11-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TENANT_STATUSES = "'provisioning', 'active', 'updating', 'destroying', 'destroyed', 'failed'"
TASK_TYPES = "'deploy', 'update', 'destroy'"
TASK_STATUSES = "'accepted', 'in_progress', 'done', 'failed'"


def upgrade() -> None:
    op.create_table(
        "tenants",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("slug", sa.String(28), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("slug", name="uq_tenants_slug"),
        sa.CheckConstraint(f"status IN ({TENANT_STATUSES})", name="ck_tenants_status"),
        sa.CheckConstraint("version >= 1", name="ck_tenants_version_positive"),
    )
    op.create_index("ix_tenants_created_at_id", "tenants", ["created_at", "id"])

    op.create_table(
        "tasks",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("type", sa.String(16), nullable=False),
        sa.Column(
            "tenant_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False
        ),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(f"type IN ({TASK_TYPES})", name="ck_tasks_type"),
        sa.CheckConstraint(f"status IN ({TASK_STATUSES})", name="ck_tasks_status"),
    )
    op.create_index("ix_tasks_created_at_id", "tasks", ["created_at", "id"])
    op.create_index("ix_tasks_tenant_id_created_at", "tasks", ["tenant_id", "created_at"])
    op.create_index(
        "uq_tasks_one_open_per_tenant",
        "tasks",
        ["tenant_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('accepted', 'in_progress')"),
    )

    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "task_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("tasks.id"), nullable=False
        ),
        sa.Column("routing_key", sa.String(64), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )

    op.create_table(
        "processed_messages",
        sa.Column("message_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("task_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("outcome", sa.String(16), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("processed_messages")
    op.drop_index("ix_outbox_unpublished", table_name="outbox")
    op.drop_table("outbox")
    op.drop_index("uq_tasks_one_open_per_tenant", table_name="tasks")
    op.drop_index("ix_tasks_tenant_id_created_at", table_name="tasks")
    op.drop_index("ix_tasks_created_at_id", table_name="tasks")
    op.drop_table("tasks")
    op.drop_index("ix_tenants_created_at_id", table_name="tenants")
    op.drop_table("tenants")
