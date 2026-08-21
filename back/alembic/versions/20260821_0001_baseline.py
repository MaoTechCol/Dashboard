"""Establish the versioned baseline for the existing dashboard schema."""

from alembic import op

from app.core.database import Base
from app import models  # noqa: F401

revision = "20260821_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    Base.metadata.create_all(bind=op.get_bind())


def downgrade() -> None:
    # The baseline is intentionally non-destructive.
    pass
