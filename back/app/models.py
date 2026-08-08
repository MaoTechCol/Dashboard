from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import Boolean, Date, DateTime, Float, Integer, String, Text, UniqueConstraint
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


class DailyMileageSnapshot(Base):
    __tablename__ = "daily_mileage_snapshots"
    __table_args__ = (
        UniqueConstraint("device_id", "snapshot_date", name="uq_daily_snapshot_device_date"),
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
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class AlarmEvent(Base):
    __tablename__ = "alarm_events"

    guid: Mapped[str] = mapped_column(String(128), primary_key=True)
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

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guid: Mapped[Optional[str]] = mapped_column(String(128), index=True, nullable=True)
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


class ReportAsset(Base):
    __tablename__ = "report_assets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_slug: Mapped[str] = mapped_column(String(64), index=True)
    year: Mapped[int] = mapped_column(Integer, index=True)
    month: Mapped[int] = mapped_column(Integer, index=True)
    original_name: Mapped[str] = mapped_column(String(255))
    stored_name: Mapped[str] = mapped_column(String(255), unique=True)
    file_path: Mapped[str] = mapped_column(String(512))
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


class IngestionAnomaly(Base):
    __tablename__ = "ingestion_anomalies"

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
    last_device_sync_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_anomaly_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)
