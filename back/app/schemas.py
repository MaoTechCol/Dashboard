from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class DashboardRules(BaseModel):
    streak_window_minutes: int = 15
    collision_window_minutes: int = 30
    yawn_window_minutes: int = 60
    eyes_closed_critical_threshold: int = 3
    collision_pattern_threshold: int = 3
    yawn_fatigue_threshold: int = 5
    fatigue_merge_window_minutes: int = 60
    echo_window_seconds: int = 60
    anti_noise_daily_cap: int = 20
    alert_close_after_hours: int = 2
    night_window_start: int = 22
    night_window_end: int = 5
    ingestion_cycle_minutes: int = 15
    feed_late_threshold_minutes: int = 20
    feed_stopped_threshold_minutes: int = 45
    spike_threshold_multiplier: float = 1.5
    fatigue_profile_min_alarms: int = 40
    night_profile_min_alarms: int = 15


class CompanyBrand(BaseModel):
    eyebrow: str
    title: str
    subtitle: str
    accent: str = "#10b981"
    warning: str = "#f97316"
    danger: str = "#ef4444"
    muted: str = "#8a90a8"


class DataQualityNote(BaseModel):
    title: str
    message: str
    start_date: date
    end_date: date | None = None
    severity: Literal["info", "warning", "critical"] = "warning"


class CompanyConfig(BaseModel):
    slug: str
    name: str
    customer: str
    timezone: str
    subdomain: str | None = None
    fleet_ids: list[str] = Field(default_factory=list)
    device_ids: list[str] = Field(default_factory=list)
    allowed_categories: list[str] = Field(default_factory=list)
    subtype_map: dict[str, str] = Field(default_factory=dict)
    plate_aliases: dict[str, str] = Field(default_factory=dict)
    notes: str | None = None
    quality_notes: list[DataQualityNote] = Field(default_factory=list)
    brand: CompanyBrand
    rules: DashboardRules = Field(default_factory=DashboardRules)


class QualityNoteView(BaseModel):
    title: str
    message: str
    severity: Literal["info", "warning", "critical"]
    start_date: date
    end_date: date | None = None


class DataQualityView(BaseModel):
    active_notes: list[QualityNoteView] = Field(default_factory=list)
    anomaly_count_24h: int = 0
    last_anomaly_at: datetime | None = None


class FeedState(BaseModel):
    status: str
    label: str
    minutes_since_last_message: int | None = None
    last_message_at: datetime | None = None
    last_cycle_received_at: datetime | None = None
    last_event_observed_at: datetime | None = None
    last_alarm_at: datetime | None = None
    last_status_at: datetime | None = None
    last_live_alarm_message_at: datetime | None = None
    last_live_dms_at: datetime | None = None
    last_live_unmapped_at: datetime | None = None
    connection_state: str
    last_error: str | None = None


class ReportFileView(BaseModel):
    year: int
    month: int
    original_name: str
    size_bytes: int
    uploaded_at: datetime
    download_url: str


class UserSessionView(BaseModel):
    username: str
    role: Literal["admin", "client"]
    company_slug: str | None = None
    company_name: str | None = None


class CompanySummaryView(BaseModel):
    slug: str
    name: str
    customer: str
    timezone: str
    brand: CompanyBrand


class AdminCompanyCatalogItemView(BaseModel):
    slug: str
    name: str
    customer: str
    timezone: str
    subdomain: str | None = None
    fleet_ids: list[str] = Field(default_factory=list)
    device_ids: list[str] = Field(default_factory=list)
    operational: bool = False
    ready_in_selector: bool = False
    rebuild_status: str = "idle"
    rebuild_progress_pct: float | None = None
    rebuild_days_done: int = 0
    rebuild_days_total: int = 0
    rebuild_started_at: datetime | None = None
    rebuild_finished_at: datetime | None = None
    rebuild_next_retry_at: datetime | None = None
    rebuild_published_cut_at: datetime | None = None
    rebuild_error_message: str | None = None
    can_deactivate: bool = True


class AdminCompanyCatalogView(BaseModel):
    total_companies: int = 0
    operational_companies: int = 0
    companies: list[AdminCompanyCatalogItemView] = Field(default_factory=list)
    activation_jobs: list[AdminCompanyCatalogItemView] = Field(default_factory=list)
    fleet_candidates: list["FleetCandidateView"] = Field(default_factory=list)


class AuthMeResponse(BaseModel):
    user: UserSessionView
    companies: list[CompanySummaryView] = Field(default_factory=list)
    selected_company_slug: str | None = None


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    ok: bool = True
    user: UserSessionView
    companies: list[CompanySummaryView] = Field(default_factory=list)
    selected_company_slug: str | None = None


class BackfillRequest(BaseModel):
    company_slug: str | None = None
    device_id: str | None = None
    start_at: datetime
    end_at: datetime
    publish_snapshot: bool = False


class HarvestRerunRequest(BaseModel):
    company_slug: str
    cut_at: datetime


class HistoricalRebuildRequest(BaseModel):
    company_slug: str
    start_date: date | None = None
    end_date: date | None = None
    days: int = 30
    publish_snapshot: bool = True
    maintenance: bool = True
    maintenance_drain_timeout: float = 90.0


class MaintenanceModeRequest(BaseModel):
    enabled: bool
    reason: str | None = None


class StatusReplayResult(BaseModel):
    processed: int = 0
    inserted: int = 0
    skipped: int = 0
    loaded: int = 0


class AdminIngestionStatusView(BaseModel):
    mode: str
    connection_state: str
    maintenance_mode: bool = False
    maintenance_reason: str | None = None
    maintenance_started_at: datetime | None = None
    last_cycle_received_at: datetime | None = None
    last_event_observed_at: datetime | None = None
    last_alarm_at: datetime | None = None
    last_status_at: datetime | None = None
    last_live_alarm_message_at: datetime | None = None
    last_live_dms_at: datetime | None = None
    last_live_unmapped_at: datetime | None = None
    last_device_sync_at: datetime | None = None
    last_error: str | None = None
    anomaly_count_24h: int = 0
    live_alarm_count_24h: int = 0
    live_dms_count_24h: int = 0
    raw_dms_count_24h: int = 0
    backfill_dms_count_24h: int = 0
    catchup_dms_count_24h: int = 0
    live_unmapped_count_24h: int = 0
    non_dms_count_24h: int = 0
    live_non_dms_count_24h: int = 0
    future_rejected_count_24h: int = 0
    live_future_rejected_count_24h: int = 0
    catchup_failures_24h: int = 0
    last_successful_catchup_cursor_at: datetime | None = None
    last_successful_catchup_observed_at: datetime | None = None
    pending_range_start_at: datetime | None = None
    pending_range_end_at: datetime | None = None
    next_catchup_retry_at: datetime | None = None
    catchup_rate_limit_streak: int = 0
    last_catchup_attempt_at: datetime | None = None
    last_catchup_error: str | None = None
    operational_recency: "OperationalRecencyView" = Field(default_factory=lambda: OperationalRecencyView())


class CoverageSummaryView(BaseModel):
    total_vehicles: int = 0
    vehicles_reporting_status_24h: int = 0
    vehicles_with_any_alarm_24h: int = 0
    vehicles_with_dms_alarm_24h: int = 0
    vehicles_with_live_dms_24h: int = 0
    vehicles_with_valid_day_km_today: int = 0
    vehicles_missing_day_km_today: int = 0
    vehicles_with_status_today: int = 0
    stale_vehicles: int = 0
    vehicles_with_snapshot_today: int = 0


class KmSummaryView(BaseModel):
    total_window_km: float = 0.0
    closed_window_km: float = 0.0
    current_day_km_provisional: float = 0.0
    current_day_label: date


class ReportsSummaryView(BaseModel):
    available_reports: int = 0
    latest_report_year: int | None = None
    latest_report_month: int | None = None


class PublicationStateView(BaseModel):
    dashboard_host: str | None = None
    dashboard_url: str | None = None
    api_url: str | None = None
    dns_status: Literal["unconfigured", "resolved", "unresolved"] = "unconfigured"
    resolved_targets: list[str] = Field(default_factory=list)
    local_validation_only: bool = True
    message: str


class OperationalRecencyView(BaseModel):
    last_raw_dms_at: datetime | None = None
    last_accepted_dms_at: datetime | None = None
    last_visible_dms_at: datetime | None = None
    last_pending_review_at: datetime | None = None
    last_pending_visibility_at: datetime | None = None
    pending_review_count: int = 0
    pending_actionable_count: int = 0
    pending_visibility_count: int = 0
    latest_pending_reason: str | None = None
    latest_pending_plate: str | None = None


class AdminOverviewView(BaseModel):
    company_slug: str
    company_name: str
    ingest_mode: str
    feed: FeedState
    coverage: CoverageSummaryView
    km: KmSummaryView
    reports: ReportsSummaryView
    publication: PublicationStateView
    anomaly_count_24h: int = 0
    active_notes: list[QualityNoteView] = Field(default_factory=list)
    operational_recency: OperationalRecencyView = Field(default_factory=OperationalRecencyView)


class RecentAuditView(BaseModel):
    raw_events: int = 0
    grouped_episodes: int = 0
    visible_alerts: int = 0
    fused_in_episode: int = 0
    dismissed_alerts: int = 0
    suppressed_by_rule: int = 0
    visible_raw_events: int = 0
    non_dms_hidden: int = 0
    unmapped_hidden: int = 0
    future_rejected: int = 0


class AlarmAuditView(BaseModel):
    accepted_total: int = 0
    visible_total: int = 0
    unclassified_total: int = 0
    mapping_sources: dict[str, int] = Field(default_factory=dict)
    by_category: dict[str, int] = Field(default_factory=dict)
    audit_stages: dict[str, int] = Field(default_factory=dict)
    audit_reasons: dict[str, int] = Field(default_factory=dict)
    by_subtype: list[dict[str, Any]] = Field(default_factory=list)


class AnomalyAuditView(BaseModel):
    total: int = 0
    by_reason: dict[str, int] = Field(default_factory=dict)


class AdminAuditView(BaseModel):
    company_slug: str
    company_name: str
    range_start: datetime
    range_end: datetime
    alarms: AlarmAuditView
    anomalies: AnomalyAuditView
    requested_window: RecentAuditView
    recent_7d: RecentAuditView
    recent_24h: RecentAuditView


class ReconciliationRunRequest(BaseModel):
    company_slug: str
    from_at: datetime = Field(alias="from")
    to_at: datetime = Field(alias="to")
    window_type: Literal["calendar_day_local", "rolling_24h", "calendar_month_local"] = "calendar_day_local"

    model_config = {"populate_by_name": True}


class ReconciliationSummary(BaseModel):
    company_slug: str
    company_name: str
    window_type: Literal["calendar_day_local", "rolling_24h", "calendar_month_local"]
    range_start: datetime
    range_end: datetime
    raw_portal_equivalent: int = 0
    ingested_live: int = 0
    ingested_backfill: int = 0
    classified_dms: int = 0
    classified_non_dms: int = 0
    visible_episodes: int = 0
    visible_raw_events: int = 0
    suppressed_by_rule: int = 0
    rejected_temporal: int = 0
    unmapped: int = 0
    missing_local: int = 0


class ReconciliationRunResponse(BaseModel):
    job_id: str
    status: str
    cached_result_available: bool = False
    total_devices: int = 0
    processed_devices: int = 0
    succeeded_devices: int = 0
    failed_devices: int = 0
    rate_limited_devices: int = 0
    current_device_id: str | None = None
    range_start: datetime
    range_end: datetime
    window_type: Literal["calendar_day_local", "rolling_24h", "calendar_month_local"]
    summary: ReconciliationSummary | None = None
    drilldown: list["ReconciliationDrilldownRow"] = Field(default_factory=list)


class ReconciliationJobView(BaseModel):
    job_id: str
    status: Literal["queued", "running", "succeeded", "failed", "rate_limited"]
    company_slug: str
    range_start: datetime
    range_end: datetime
    window_type: Literal["calendar_day_local", "rolling_24h", "calendar_month_local"]
    cached_result_available: bool = False
    total_devices: int = 0
    processed_devices: int = 0
    succeeded_devices: int = 0
    failed_devices: int = 0
    rate_limited_devices: int = 0
    current_device_id: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None
    summary: ReconciliationSummary | None = None
    drilldown: list["ReconciliationDrilldownRow"] = Field(default_factory=list)


class ReconciliationDrilldownRow(BaseModel):
    guid: str
    plate_no: str | None = None
    device_id: str | None = None
    observed_hour_local: str | None = None
    portal_begin_time: str | None = None
    portal_reporting_time: str | None = None
    raw_alarm_type: str | None = None
    raw_tp: str | None = None
    raw_event_code: str | None = None
    observed_at: datetime | None = None
    stored_observed_at: datetime | None = None
    stored_raw_event_time: str | None = None
    classification_status: str
    visibility_status: str
    source: str
    category: str | None = None
    subtype: str | None = None
    reason: str
    episode_guid: str | None = None
    episode_title: str | None = None
    portal_duplicate_count: int = 1
    diagnostic_note: str | None = None


class ReconciliationReviewDecisionRequest(BaseModel):
    note: str | None = None


class ReconciliationReviewBulkDecisionRequest(BaseModel):
    ids: list[int] = Field(default_factory=list)
    note: str | None = None


class ReconciliationReviewItemView(BaseModel):
    id: int
    company_slug: str
    guid: str | None = None
    device_id: str | None = None
    plate_no: str | None = None
    observed_at: datetime | None = None
    portal_begin_time: str | None = None
    portal_reporting_time: str | None = None
    raw_alarm_type: str | None = None
    raw_tp: str | None = None
    raw_event_code: str | None = None
    classification_status: str | None = None
    visibility_status: str | None = None
    category: str | None = None
    subtype: str | None = None
    reason: str
    diagnostic_note: str | None = None
    suggested_action: str = "reconcile"
    review_status: str = "pending"
    source_job_id: str | None = None
    source_window_type: str | None = None
    decision_note: str | None = None
    decided_by: str | None = None
    decided_at: datetime | None = None
    applied_at: datetime | None = None


class ReconciliationReviewListView(BaseModel):
    total_items: int = 0
    counts_by_action: dict[str, int] = Field(default_factory=dict)
    counts_by_reason: dict[str, int] = Field(default_factory=dict)
    items: list[ReconciliationReviewItemView] = Field(default_factory=list)


class ReconciliationReviewBulkDecisionResponse(BaseModel):
    updated: int = 0
    items: list[ReconciliationReviewItemView] = Field(default_factory=list)


class KmRepairRequest(BaseModel):
    company_slug: str
    start_date: date | None = None
    end_date: date | None = None


class KmQualitySummary(BaseModel):
    company_slug: str
    company_name: str
    total_vehicles: int = 0
    vehicles_with_valid_day_km: int = 0
    vehicles_with_invalid_day_km: int = 0
    vehicles_with_total_regression: int = 0
    vehicles_with_snapshot_today: int = 0
    vehicles_with_status_today: int = 0
    current_day_km_source: str = "device_state_validated"
    repaired_rows: int = 0
    sample_invalid_vehicles: list[str] = Field(default_factory=list)
    sample_total_regression_vehicles: list[str] = Field(default_factory=list)
    sample_missing_day_km_vehicles: list[str] = Field(default_factory=list)


class AdminVehicleView(BaseModel):
    device_id: str
    plate_no: str | None = None
    fleet_id: str | None = None
    fleet_name: str | None = None
    device_name: str | None = None
    driver_name: str | None = None
    last_received_at: datetime | None = None
    last_seen_at: datetime | None = None
    last_alarm_at: datetime | None = None
    last_total_km: float | None = None
    last_day_km: float | None = None
    last_snapshot_total_km: float | None = None
    last_snapshot_day_km: float | None = None
    last_snapshot_at: datetime | None = None
    feed_status: str
    record_source: str | None = None


class IngestionAnomalyView(BaseModel):
    id: int
    source_type: str
    device_id: str | None = None
    company_slug: str | None = None
    received_at: datetime
    raw_event_time: str | None = None
    reason: str
    payload_json: str


class CompanyAssignmentRequest(BaseModel):
    company_slug: str
    fleet_ids: list[str] = Field(default_factory=list)
    device_ids: list[str] = Field(default_factory=list)


class CompanyActivationRequest(BaseModel):
    slug: str
    name: str
    customer: str | None = None
    timezone: str = "America/Bogota"
    subdomain: str | None = None
    fleet_ids: list[str] = Field(default_factory=list)
    device_ids: list[str] = Field(default_factory=list)
    notes: str | None = None
    client_password: str = Field(min_length=1)


class AdminPasswordChangeRequest(BaseModel):
    new_password: str = Field(min_length=1)


class CompanyPasswordChangeRequest(BaseModel):
    company_slug: str
    new_password: str = Field(min_length=1)


class CompanyAssignmentView(BaseModel):
    company_slug: str
    fleet_ids: list[str] = Field(default_factory=list)
    device_ids: list[str] = Field(default_factory=list)
    points_to_mock: bool = False
    visible_devices: int = 0
    visible_mock_devices: int = 0
    visible_real_devices: int = 0
    visible_snapshots: int = 0
    visible_mock_snapshots: int = 0
    visible_real_snapshots: int = 0
    visible_alarms: int = 0
    visible_mock_alarms: int = 0
    visible_real_alarms: int = 0


class FleetCandidateView(BaseModel):
    fleet_id: str
    fleet_name: str | None = None
    total_devices: int = 0
    devices_with_status: int = 0
    devices_seen_24h: int = 0
    alarm_events_7d: int = 0
    latest_seen_at: datetime | None = None
    latest_alarm_at: datetime | None = None
    sample_plates: list[str] = Field(default_factory=list)
    selected: bool = False
    assigned_company_slug: str | None = None
    assigned_company_name: str | None = None


class MockDataSummaryView(BaseModel):
    devices_total: int = 0
    devices_mock: int = 0
    devices_real: int = 0
    snapshots_total: int = 0
    snapshots_mock: int = 0
    snapshots_real: int = 0
    alarms_total: int = 0
    alarms_mock: int = 0
    alarms_real: int = 0


class UnclassifiedCodeView(BaseModel):
    subtype: str | None = None
    event_code: str | None = None
    count: int = 0
    sample_device_id: str | None = None
    sample_plate: str | None = None


class RawAlarmDiagnosticView(BaseModel):
    guid: str
    source: str
    occurred_at: datetime | None = None
    received_at: datetime
    device_id: str | None = None
    plate_no: str | None = None
    raw_alarm_type: str | None = None
    raw_tp: str | None = None
    raw_event_code: str | None = None
    classification_status: str | None = None
    mapped_category: str | None = None
    mapping_source: str | None = None
    temporal_status: str | None = None
    ingest_result: str | None = None


class AdminLiveSetupView(BaseModel):
    company_slug: str
    company_name: str
    assignment: CompanyAssignmentView
    mock_data: MockDataSummaryView
    fleet_candidates: list[FleetCandidateView] = Field(default_factory=list)
    unclassified_codes: list[UnclassifiedCodeView] = Field(default_factory=list)
    recent_raw_diagnostics: list[RawAlarmDiagnosticView] = Field(default_factory=list)


class MockDataPurgeResult(BaseModel):
    deleted_devices: int = 0
    deleted_snapshots: int = 0
    deleted_alarms: int = 0
    deleted_mileage_readings: int = 0


class FeedSocketPayload(BaseModel):
    company_slug: str
    company_name: str
    connection_state: str
    feed_status: str
    feed_label: str
    last_cycle_received_at: datetime | None = None
    last_event_observed_at: datetime | None = None
    last_error: str | None = None
    new_cycle_available: bool = False
    anomaly_count_24h: int = 0


class NormalizedStatus(BaseModel):
    device_id: str
    observed_at: datetime
    total_km: float | None = None
    day_km: float | None = None
    plate_no: str | None = None
    fleet_id: str | None = None
    driver_name: str | None = None
    device_name: str | None = None
    raw_event_time: str | None = None
    raw_total_value: str | None = None
    raw_day_value: str | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


class NormalizedAlarm(BaseModel):
    guid: str
    device_id: str
    occurred_at: datetime
    category: str = "Sin clasificar"
    subtype: str | None = None
    mapping_source: str | None = None
    event_code: str | None = None
    raw_alarm_type: str | None = None
    raw_tp: str | None = None
    raw_event_code: str | None = None
    classification_status: Literal["classified_dms", "classified_non_dms", "unmapped"] = "unmapped"
    visibility_status: str = "hidden_unmapped"
    start_at: datetime | None = None
    end_at: datetime | None = None
    plate_no: str | None = None
    fleet_id: str | None = None
    driver_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    total_mileage_km: float | None = None
    raw_event_time: str | None = None
    raw: dict[str, Any] = Field(default_factory=dict)
