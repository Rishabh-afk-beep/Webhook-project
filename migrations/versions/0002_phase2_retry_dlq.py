"""Phase 2: retry policy, dead-letter queue columns and indexes.

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-30
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ------------------------------------------------------------------ #
    # event_deliveries: update status CHECK constraint                     #
    # ------------------------------------------------------------------ #
    # PostgreSQL requires explicit DROP + ADD for named CHECK constraints.
    op.drop_constraint(
        "ck_event_deliveries_status",
        "event_deliveries",
        type_="check",
    )
    op.create_check_constraint(
        "ck_event_deliveries_status",
        "event_deliveries",
        "status IN ('pending', 'processing', 'retry_scheduled', 'succeeded', 'failed', 'dead_lettered')",
    )

    # ------------------------------------------------------------------ #
    # event_deliveries: new columns                                        #
    # ------------------------------------------------------------------ #
    op.add_column(
        "event_deliveries",
        sa.Column("dead_lettered_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "event_deliveries",
        sa.Column("replayed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "event_deliveries",
        sa.Column(
            "replay_of_delivery_id",
            sa.Uuid(),
            sa.ForeignKey("event_deliveries.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )

    # ------------------------------------------------------------------ #
    # delivery_attempts: new columns                                       #
    # ------------------------------------------------------------------ #
    op.add_column(
        "delivery_attempts",
        sa.Column("error_category", sa.String(50), nullable=True),
    )

    # ------------------------------------------------------------------ #
    # destinations: rate-limiting columns                                  #
    # ------------------------------------------------------------------ #
    op.add_column(
        "destinations",
        sa.Column("rate_limit_per_second", sa.Float(), nullable=True),
    )
    op.add_column(
        "destinations",
        sa.Column("rate_limit_burst", sa.Integer(), nullable=True),
    )

    # ------------------------------------------------------------------ #
    # Indexes for retry and DLQ query paths                                #
    # ------------------------------------------------------------------ #
    op.create_index(
        "ix_event_deliveries_next_attempt_at",
        "event_deliveries",
        ["status", "next_attempt_at"],
        postgresql_where=sa.text("status = 'retry_scheduled'"),
    )
    op.create_index(
        "ix_event_deliveries_dead_lettered",
        "event_deliveries",
        ["tenant_id", "dead_lettered_at"],
        postgresql_where=sa.text("status = 'dead_lettered'"),
    )


def downgrade() -> None:
    op.drop_index("ix_event_deliveries_dead_lettered", table_name="event_deliveries")
    op.drop_index("ix_event_deliveries_next_attempt_at", table_name="event_deliveries")

    op.drop_column("destinations", "rate_limit_burst")
    op.drop_column("destinations", "rate_limit_per_second")

    op.drop_column("delivery_attempts", "error_category")

    op.drop_column("event_deliveries", "replay_of_delivery_id")
    op.drop_column("event_deliveries", "replayed_at")
    op.drop_column("event_deliveries", "dead_lettered_at")

    op.drop_constraint("ck_event_deliveries_status", "event_deliveries", type_="check")
    op.create_check_constraint(
        "ck_event_deliveries_status",
        "event_deliveries",
        "status IN ('pending', 'processing', 'succeeded', 'failed')",
    )
