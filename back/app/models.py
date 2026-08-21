from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import Boolean, Date, DateTime, Float, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base
from app.core.time import utc_now


class DeviceRecord(Base):
    __tablename__ = "devices"

    device_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    plate_no: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    company_slug: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    fleet_id: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    fleet_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    device_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    driver_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_received_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_total_km: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    last_day_km: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    raw_total_value: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    raw_day_value: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    km_validation_status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    km_validation_reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    record_source: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    raw_payload: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class MileageReading(Base):
    __tablename__ = "mileage_readings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[str] = mapped_column(String(128), index=True)
    plate_no: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    fleet_id: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    total_km: Mapped[float] = mapped_column(Float)
    day_km: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(String(32), default="status")


class MileageObservation(Base):
    __tablename__ = "mileage_observations"
    __table_args__ = (
        UniqueConstraint("observation_key", name="uq_mileage_observations_key"),
        Index("ix_mileage_observations_company_observed", "company_slug", "observed_at"),
        Index(
            "ix_mileage_observations_company_device_observed",
            "company_slug",
            "device_id",
            "observed_at",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    observation_key: Mapped[str] = mapped_column(String(255), index=True)
    company_slug: Mapped[str] = mapped_column(String(64), index=True)
    device_id: Mapped[str] = mapped_column(String(128), index=True)
    fleet_id: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    plate_no: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    source: Mapped[str] = mapped_column(String(32), index=True)
    total_km: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    day_km: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    raw_total_value: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    raw_day_value: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    validation_status: Mapped[str] = mapped_column(String(32), index=True, default="valid")
    validation_reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class DailyMileageSnapshot(Base):
    __tablename__ = "daily_mileage_snapshots"
    __table_args__ = (
        UniqueConstraint("device_id", "snapshot_date", name="uq_daily_snapshot_device_date"),
        Index("ix_daily_snapshots_company_date", "company_slug", "snapshot_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[str] = mapped_column(String(128), index=True)
    plate_no: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    company_slug: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    fleet_id: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    snapshot_date: Mapped[date] = mapped_column(Date, index=True)
    total_km: Mapped[float] = mapped_column(Float)
    day_km: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    raw_total_value: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    raw_day_value: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    km_validation_status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    km_validation_reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    repair_reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    repaired_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    source: Mapped[str] = mapped_column(String(32), default="live")
    closure_status: Mapped[str] = mapped_column(String(32), index=True, default="provisional")
    coverage_status: Mapped[str] = mapped_column(String(32), index=True, default="covered")
    sample_count: Mapped[int] = mapped_column(Integer, default=0)
    first_observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    late_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    approved_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    excluded_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    excluded_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class AlarmEvent(Base):
    __tablename__ = "alarm_events"
    __table_args__ = (
        UniqueConstraint("provider_event_key", name="uq_alarm_events_provider_event_key"),
        Index("ix_alarm_events_company_device_occurred", "company_slug", "device_id", "occurred_at"),
        Index("ix_alarm_events_company_occurred", "company_slug", "occurred_at"),
        Index("ix_alarm_events_company_visibility_occurred", "company_slug", "visibility_status", "occurred_at"),
    )

    guid: Mapped[str] = mapped_column(String(128), primary_key=True)
    provider_event_key: Mapped[Optional[str]] = mapped_column(String(255), index=True, nullable=True)
    device_id: Mapped[str] = mapped_column(String(128), index=True)
    plate_no: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    company_slug: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    fleet_id: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    driver_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    category: Mapped[str] = mapped_column(String(64), index=True)
    subtype: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    mapping_source: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    classification_status: Mapped[Optional[str]] = mapped_column(String(32), index=True, nullable=True)
    visibility_status: Mapped[Optional[str]] = mapped_column(String(32), index=True, nullable=True)
    event_code: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    raw_alarm_type: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    raw_tp: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    raw_event_code: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    received_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True, nullable=True)
    start_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    end_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    raw_event_time: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    latitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    longitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    total_mileage_km: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(String(32), default="live")
    raw_payload: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class AlarmEventAudit(Base):
    __tablename__ = "alarm_event_audit"
    __table_args__ = (
        Index("ix_alarm_event_audit_company_device_received", "company_slug", "device_id", "received_at"),
        Index("ix_alarm_event_audit_company_received", "company_slug", "received_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guid: Mapped[Optional[str]] = mapped_column(String(128), index=True, nullable=True)
    provider_event_key: Mapped[Optional[str]] = mapped_column(String(255), index=True, nullable=True)
    company_slug: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    device_id: Mapped[Optional[str]] = mapped_column(String(128), index=True, nullable=True)
    fleet_id: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    plate_no: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True, nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    raw_alarm_type: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    raw_tp: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    raw_event_code: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    stage: Mapped[str] = mapped_column(String(64), index=True)
    reason: Mapped[str] = mapped_column(String(255), index=True)
    payload_json: Mapped[str] = mapped_column(Text)


class HowenAlarmRaw(Base):
    __tablename__ = "howen_alarm_raw"
    __table_args__ = (
        UniqueConstraint("provider_event_key", name="uq_howen_alarm_raw_provider_event_key"),
        Index("ix_howen_alarm_raw_company_device_occurred", "company_slug", "device_id", "occurred_at"),
        Index("ix_howen_alarm_raw_company_occurred", "company_slug", "occurred_at"),
        Index("ix_howen_alarm_raw_company_received", "company_slug", "received_at"),
        Index(
            "ix_howen_alarm_raw_company_classification_occurred",
            "company_slug",
            "classification_status",
            "occurred_at",
        ),
    )

    guid: Mapped[str] = mapped_column(String(128), primary_key=True)
    provider_event_key: Mapped[Optional[str]] = mapped_column(String(255), index=True, nullable=True)
    company_slug: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    device_id: Mapped[Optional[str]] = mapped_column(String(128), index=True, nullable=True)
    fleet_id: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    plate_no: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    source: Mapped[str] = mapped_column(String(32), index=True, default="live")
    occurred_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True, nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    raw_alarm_type: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    raw_tp: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    raw_event_code: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    raw_event_time: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    classification_status: Mapped[Optional[str]] = mapped_column(String(32), index=True, nullable=True)
    mapped_category: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    mapping_source: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    temporal_status: Mapped[Optional[str]] = mapped_column(String(32), index=True, nullable=True)
    ingest_result: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    payload_json: Mapped[str] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class ReportAsset(Base):
    __tablename__ = "report_assets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_slug: Mapped[str] = mapped_column(String(64), index=True)
    year: Mapped[int] = mapped_column(Integer, index=True)
    month: Mapped[int] = mapped_column(Integer, index=True)
    original_name: Mapped[str] = mapped_column(String(255))
    stored_name: Mapped[str] = mapped_column(String(255), unique=True)
    file_path: Mapped[str] = mapped_column(String(512))
    storage_backend: Mapped[str] = mapped_column(String(32), default="local")
    storage_key: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    size_bytes: Mapped[int] = mapped_column(Integer)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class UserAccount(Base):
    __tablename__ = "user_accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(16), index=True)
    company_slug: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class ManagedCompany(Base):
    __tablename__ = "managed_companies"

    slug: Mapped[str] = mapped_column(String(64), primary_key=True)
    config_json: Mapped[str] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class SystemSetting(Base):
    __tablename__ = "system_settings"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value_json: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class CompanyDailyAggregate(Base):
    __tablename__ = "company_daily_aggregates"
    __table_args__ = (
        UniqueConstraint("company_slug", "aggregate_date", name="uq_company_daily_aggregate"),
        Index("ix_company_daily_aggregates_company_date", "company_slug", "aggregate_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_slug: Mapped[str] = mapped_column(String(64), index=True)
    aggregate_date: Mapped[date] = mapped_column(Date, index=True)
    snapshot_version: Mapped[Optional[str]] = mapped_column(String(128), index=True, nullable=True)
    metrics_json: Mapped[str] = mapped_column(Text, default="{}")
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class CompanyWindowAggregate(Base):
    __tablename__ = "company_window_aggregates"
    __table_args__ = (
        UniqueConstraint(
            "company_slug",
            "window_type",
            "range_start",
            "range_end",
            "snapshot_version",
            name="uq_company_window_aggregate_version",
        ),
        Index(
            "ix_company_window_aggregates_lookup",
            "company_slug",
            "window_type",
            "range_start",
            "range_end",
            "snapshot_version",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_slug: Mapped[str] = mapped_column(String(64), index=True)
    window_type: Mapped[str] = mapped_column(String(32), index=True)
    range_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    range_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    snapshot_version: Mapped[str] = mapped_column(String(128), index=True)
    payload_json: Mapped[str] = mapped_column(Text)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, index=True)


class DataCertificationRun(Base):
    __tablename__ = "data_certification_runs"
    __table_args__ = (
        Index("ix_data_certification_company_range", "company_slug", "range_start", "range_end"),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    company_slug: Mapped[str] = mapped_column(String(64), index=True)
    source_name: Mapped[str] = mapped_column(String(255))
    range_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    range_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    provider_alarm_count: Mapped[int] = mapped_column(Integer, default=0)
    local_raw_count: Mapped[int] = mapped_column(Integer, default=0)
    local_analytic_count: Mapped[int] = mapped_column(Integer, default=0)
    unexplained_alarm_count: Mapped[int] = mapped_column(Integer, default=0)
    provider_km: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    local_km: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    km_difference_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    result_json: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class IngestionAnomaly(Base):
    __tablename__ = "ingestion_anomalies"
    __table_args__ = (
        Index("ix_ingestion_anomalies_company_received", "company_slug", "received_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_type: Mapped[str] = mapped_column(String(32), index=True)
    device_id: Mapped[Optional[str]] = mapped_column(String(128), index=True, nullable=True)
    company_slug: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    raw_event_time: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    reason: Mapped[str] = mapped_column(String(255), index=True)
    payload_json: Mapped[str] = mapped_column(Text)


class IngestState(Base):
    __tablename__ = "ingest_state"

    key: Mapped[str] = mapped_column(String(32), primary_key=True, default="global")
    mode: Mapped[str] = mapped_column(String(16), default="live")
    connection_state: Mapped[str] = mapped_column(String(32), default="idle")
    last_message_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_cycle_received_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_event_observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_status_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_alarm_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_live_alarm_message_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_live_dms_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_live_unmapped_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_device_sync_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_anomaly_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    maintenance_mode: Mapped[bool] = mapped_column(Boolean, default=False)
    maintenance_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    maintenance_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class PublishedDashboardSnapshot(Base):
    __tablename__ = "published_dashboard_snapshots"

    company_slug: Mapped[str] = mapped_column(String(64), primary_key=True)
    published_cut_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True, nullable=True)
    next_cut_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True, nullable=True)
    window_start: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    window_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    cut_status: Mapped[str] = mapped_column(String(32), index=True, default="pending")
    last_completed_harvest_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_status_message_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_status_observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_dms_published_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    snapshot_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class CatchupCursor(Base):
    __tablename__ = "catchup_cursor"

    company_slug: Mapped[str] = mapped_column(String(64), primary_key=True)
    last_successful_catchup_cursor_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_successful_catchup_observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    pending_range_start_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    pending_range_end_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    next_device_offset: Mapped[int] = mapped_column(Integer, default=0)
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    rate_limit_streak: Mapped[int] = mapped_column(Integer, default=0)
    last_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class CompanyHistoricalRebuildJob(Base):
    __tablename__ = "company_historical_rebuild_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_slug: Mapped[str] = mapped_column(String(64), index=True)
    purpose: Mapped[str] = mapped_column(String(32), index=True, default="activation_bootstrap")
    status: Mapped[str] = mapped_column(String(32), index=True, default="queued")
    start_date: Mapped[date] = mapped_column(Date, index=True)
    end_date: Mapped[date] = mapped_column(Date, index=True)
    days_total: Mapped[int] = mapped_column(Integer, default=0)
    days_done: Mapped[int] = mapped_column(Integer, default=0)
    devices_total: Mapped[int] = mapped_column(Integer, default=0)
    phase: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    rows_total: Mapped[int] = mapped_column(Integer, default=0)
    rows_processed: Mapped[int] = mapped_column(Integer, default=0)
    current_device_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    current_device_offset: Mapped[int] = mapped_column(Integer, default=0)
    mileage_days_total: Mapped[int] = mapped_column(Integer, default=0)
    mileage_days_done: Mapped[int] = mapped_column(Integer, default=0)
    mileage_devices_total: Mapped[int] = mapped_column(Integer, default=0)
    mileage_devices_done: Mapped[int] = mapped_column(Integer, default=0)
    mileage_rows_total: Mapped[int] = mapped_column(Integer, default=0)
    mileage_rows_processed: Mapped[int] = mapped_column(Integer, default=0)
    mileage_valid_days: Mapped[int] = mapped_column(Integer, default=0)
    mileage_missing_days: Mapped[int] = mapped_column(Integer, default=0)
    mileage_coverage_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    last_mileage_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    degraded_publication_approved: Mapped[bool] = mapped_column(Boolean, default=False)
    degraded_publication_approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    degraded_publication_approved_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    last_heartbeat_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    inserted: Mapped[int] = mapped_column(Integer, default=0)
    anomalies: Mapped[int] = mapped_column(Integer, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, default=0)
    last_processed_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    published_cut_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class ReconciliationJob(Base):
    __tablename__ = "reconciliation_jobs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    company_slug: Mapped[str] = mapped_column(String(64), index=True)
    params_hash: Mapped[str] = mapped_column(String(128), index=True)
    window_type: Mapped[str] = mapped_column(String(32), index=True)
    range_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    range_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True, default="queued")
    summary_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    drilldown_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    result_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class CompanyLifecycleAudit(Base):
    __tablename__ = "company_lifecycle_audit"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_slug: Mapped[str] = mapped_column(String(64), index=True)
    action: Mapped[str] = mapped_column(String(32), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    requested_by: Mapped[str] = mapped_column(String(128), index=True)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    backup_path: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    backup_sha256: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    row_count: Mapped[int] = mapped_column(Integer, default=0)
    detail_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class AlarmHarvestRun(Base):
    __tablename__ = "alarm_harvest_runs"
    __table_args__ = (
        UniqueConstraint("company_slug", "cut_at", name="uq_alarm_harvest_run_company_cut"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_slug: Mapped[str] = mapped_column(String(64), index=True)
    cut_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True, default="queued")
    devices_total: Mapped[int] = mapped_column(Integer, default=0)
    devices_done: Mapped[int] = mapped_column(Integer, default=0)
    rows_total: Mapped[int] = mapped_column(Integer, default=0)
    dms_total: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class AlarmHarvestDevice(Base):
    __tablename__ = "alarm_harvest_devices"
    __table_args__ = (
        UniqueConstraint("run_id", "device_id", name="uq_alarm_harvest_device"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(Integer, index=True)
    device_id: Mapped[str] = mapped_column(String(128), index=True)
    plate_no: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    status: Mapped[str] = mapped_column(String(32), index=True, default="queued")
    provider_rows: Mapped[int] = mapped_column(Integer, default=0)
    provider_dms_rows: Mapped[int] = mapped_column(Integer, default=0)
    inserted_raw: Mapped[int] = mapped_column(Integer, default=0)
    inserted_dms: Mapped[int] = mapped_column(Integer, default=0)
    future_rejected: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class BackgroundJob(Base):
    __tablename__ = "background_jobs"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_background_jobs_idempotency_key"),
        Index("ix_background_jobs_claim", "status", "next_attempt_at", "priority", "created_at"),
        Index("ix_background_jobs_company_type", "company_slug", "job_type", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    job_type: Mapped[str] = mapped_column(String(64), index=True)
    company_slug: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    priority: Mapped[int] = mapped_column(Integer, index=True, default=0)
    status: Mapped[str] = mapped_column(String(32), index=True, default="queued")
    payload_json: Mapped[str] = mapped_column(Text, default="{}")
    result_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    lease_owner: Mapped[Optional[str]] = mapped_column(String(128), index=True, nullable=True)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True, nullable=True)
    heartbeat_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=5)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, default=utc_now)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class ReconciliationJobDevice(Base):
    __tablename__ = "reconciliation_job_devices"
    __table_args__ = (
        UniqueConstraint("job_id", "device_id", name="uq_reconciliation_job_device"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(64), index=True)
    device_id: Mapped[str] = mapped_column(String(128), index=True)
    plate_no: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    status: Mapped[str] = mapped_column(String(32), index=True, default="queued")
    row_count: Mapped[int] = mapped_column(Integer, default=0)
    portal_rows_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class ReconciliationReview(Base):
    __tablename__ = "reconciliation_reviews"
    __table_args__ = (
        Index("ix_reconciliation_reviews_company_observed", "company_slug", "observed_at"),
        Index("ix_reconciliation_reviews_company_created", "company_slug", "created_at"),
        Index(
            "ix_reconciliation_reviews_company_status_observed",
            "company_slug",
            "review_status",
            "observed_at",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    review_key: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    company_slug: Mapped[str] = mapped_column(String(64), index=True)
    guid: Mapped[Optional[str]] = mapped_column(String(128), index=True, nullable=True)
    device_id: Mapped[Optional[str]] = mapped_column(String(128), index=True, nullable=True)
    plate_no: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True, nullable=True)
    portal_begin_time: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    portal_reporting_time: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    raw_alarm_type: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    raw_tp: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    raw_event_code: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    classification_status: Mapped[Optional[str]] = mapped_column(String(32), index=True, nullable=True)
    visibility_status: Mapped[Optional[str]] = mapped_column(String(32), index=True, nullable=True)
    category: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    subtype: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    reason: Mapped[str] = mapped_column(String(255), index=True)
    diagnostic_note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    suggested_action: Mapped[str] = mapped_column(String(32), default="reconcile")
    review_status: Mapped[str] = mapped_column(String(32), index=True, default="pending")
    source_job_id: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    source_window_type: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    portal_payload_json: Mapped[str] = mapped_column(Text)
    decision_note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    decided_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    decided_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    applied_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)
