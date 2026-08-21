"""Add the composite indexes used by diagnostics and certification."""

from alembic import op

revision = "20260821_0003"
down_revision = "20260821_0002"
branch_labels = None
depends_on = None


INDEXES = (
    (
        "ix_alarm_events_company_visibility_occurred",
        "alarm_events",
        ("company_slug", "visibility_status", "occurred_at"),
    ),
    (
        "ix_howen_alarm_raw_company_classification_occurred",
        "howen_alarm_raw",
        ("company_slug", "classification_status", "occurred_at"),
    ),
    (
        "ix_reconciliation_reviews_company_status_observed",
        "reconciliation_reviews",
        ("company_slug", "review_status", "observed_at"),
    ),
)


def upgrade() -> None:
    for name, table, columns in INDEXES:
        quoted_columns = ", ".join(f'"{column}"' for column in columns)
        op.execute(f'CREATE INDEX IF NOT EXISTS "{name}" ON "{table}" ({quoted_columns})')


def downgrade() -> None:
    for name, _, _ in reversed(INDEXES):
        op.execute(f'DROP INDEX IF EXISTS "{name}"')
