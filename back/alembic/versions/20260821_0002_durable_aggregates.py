"""Add durable aggregates, certification, and report storage metadata."""

from alembic import op
import sqlalchemy as sa

from app.core.database import Base
from app import models  # noqa: F401

revision = "20260821_0002"
down_revision = "20260821_0001"
branch_labels = None
depends_on = None


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {column["name"] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    Base.metadata.create_all(bind=op.get_bind())
    columns = _column_names("report_assets")
    if "storage_backend" not in columns:
        op.add_column(
            "report_assets",
            sa.Column("storage_backend", sa.String(length=32), nullable=False, server_default="local"),
        )
    if "storage_key" not in columns:
        op.add_column("report_assets", sa.Column("storage_key", sa.String(length=512), nullable=True))


def downgrade() -> None:
    # Production data and certification evidence are retained on rollback.
    pass
