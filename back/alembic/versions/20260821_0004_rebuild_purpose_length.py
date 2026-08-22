"""Allow descriptive purposes for durable historical rebuild jobs."""

from alembic import op
import sqlalchemy as sa


revision = "20260821_0004"
down_revision = "20260821_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("company_historical_rebuild_jobs") as batch_op:
        batch_op.alter_column(
            "purpose",
            existing_type=sa.String(length=32),
            type_=sa.String(length=64),
            existing_nullable=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("company_historical_rebuild_jobs") as batch_op:
        batch_op.alter_column(
            "purpose",
            existing_type=sa.String(length=64),
            type_=sa.String(length=32),
            existing_nullable=False,
        )
