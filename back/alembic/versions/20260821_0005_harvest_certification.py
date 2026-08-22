"""Persist per-cut and per-device DMS certification counters."""

from alembic import op
import sqlalchemy as sa


revision = "20260821_0005"
down_revision = "20260821_0004"
branch_labels = None
depends_on = None


RUN_COLUMNS = (
    "provider_unique_dms_total",
    "local_raw_dms_total",
    "local_analytic_dms_total",
    "temporal_dms_total",
    "unexplained_dms_total",
)

DEVICE_COLUMNS = (
    "provider_unique_dms_rows",
    "local_raw_dms_rows",
    "local_analytic_dms_rows",
    "temporal_dms_rows",
    "unexplained_dms_rows",
)


def upgrade() -> None:
    for column_name in RUN_COLUMNS:
        op.add_column(
            "alarm_harvest_runs",
            sa.Column(column_name, sa.Integer(), nullable=False, server_default="0"),
        )
    for column_name in DEVICE_COLUMNS:
        op.add_column(
            "alarm_harvest_devices",
            sa.Column(column_name, sa.Integer(), nullable=False, server_default="0"),
        )
    op.create_index(
        "ix_alarm_harvest_devices_run_status",
        "alarm_harvest_devices",
        ["run_id", "status"],
    )


def downgrade() -> None:
    op.drop_index("ix_alarm_harvest_devices_run_status", table_name="alarm_harvest_devices")
    for column_name in reversed(DEVICE_COLUMNS):
        op.drop_column("alarm_harvest_devices", column_name)
    for column_name in reversed(RUN_COLUMNS):
        op.drop_column("alarm_harvest_runs", column_name)
