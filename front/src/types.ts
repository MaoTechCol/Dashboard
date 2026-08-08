export type TimelineFilter = "todas" | "critico" | "alto" | "noche";

export interface CompanyBrand {
  eyebrow: string;
  title: string;
  subtitle: string;
  accent: string;
  warning: string;
  danger: string;
  muted: string;
}

export interface CompanySummary {
  slug: string;
  name: string;
  customer: string;
  timezone: string;
  brand: CompanyBrand;
}

export interface SessionUser {
  username: string;
  role: "admin" | "client";
  company_slug: string | null;
  company_name: string | null;
}

export interface AuthMeResponse {
  user: SessionUser;
  companies: CompanySummary[];
  selected_company_slug: string | null;
}

export interface FeedState {
  status: "sin_datos" | "al_dia" | "atrasado" | "detenido";
  label: string;
  minutes_since_last_message: number | null;
  last_message_at: string | null;
  last_cycle_received_at: string | null;
  last_event_observed_at: string | null;
  last_alarm_at: string | null;
  last_status_at: string | null;
  connection_state: string;
  last_error: string | null;
}

export interface DataQualityNote {
  title: string;
  message: string;
  severity: "info" | "warning" | "critical";
  start_date: string;
  end_date: string | null;
}

export interface DataQuality {
  active_notes: DataQualityNote[];
  anomaly_count_24h: number;
  last_anomaly_at: string | null;
}

export interface DashboardRules {
  streak_window_minutes: number;
  collision_window_minutes: number;
  yawn_window_minutes: number;
  eyes_closed_critical_threshold: number;
  collision_pattern_threshold: number;
  yawn_fatigue_threshold: number;
  fatigue_merge_window_minutes: number;
  echo_window_seconds: number;
  anti_noise_daily_cap: number;
  alert_close_after_hours: number;
  night_window_start: number;
  night_window_end: number;
  ingestion_cycle_minutes: number;
  feed_late_threshold_minutes: number;
  feed_stopped_threshold_minutes: number;
  spike_threshold_multiplier: number;
  fatigue_profile_min_alarms: number;
  night_profile_min_alarms: number;
}

export interface VehicleSummary {
  placa: string;
  total: number;
  baseline: number;
  spike: boolean;
}

export interface VehicleTableRow {
  placa: string;
  total: number;
  km: number;
  por100km: number | null;
  riesgo100km: number | null;
  nocturno: number;
  cats: Record<string, number>;
  baseline: number;
  spike: boolean;
}

export interface DmsSnapshot {
  ultimo: {
    total: number;
    baseline_promedio: number;
    delta_pct: number | null;
    por_cat: Record<string, number>;
    por_vehiculo: VehicleSummary[];
  };
  kpis: {
    total: number;
    critico: number;
    alto: number;
    medio: number;
    km: number;
    por100km: number | null;
    nocturno_pct: number;
    rango: string;
  };
  semana: {
    veh: string[];
    cat_veh: Record<string, number[]>;
    fechas: string[];
    linea_veh: Record<string, number[]>;
    total: number;
  };
  fechas: string[];
  serie_cat: Record<string, number[]>;
  km_dia: number[];
  dist_tipo: Array<{ tipo: string; cat: string; n: number }>;
  cat_order: string[];
  heat: Record<string, number[]>;
  tabla: VehicleTableRow[];
}

export interface RecentEvent {
  guid: string;
  deviceId: string;
  plate: string;
  category: string;
  subtype: string | null;
  occurredAt: string;
  driverName: string | null;
  latitude: number | null;
  longitude: number | null;
  totalMileageKm: number | null;
}

export interface ReportFile {
  year: number;
  month: number;
  original_name: string;
  size_bytes: number;
  uploaded_at: string;
  download_url: string;
}

export interface DashboardSnapshot {
  meta: {
    companySlug: string;
    companyName: string;
    customer: string;
    brand: CompanyBrand;
    generatedAt: string;
    timezone: string;
    rangeStart: string;
    rangeEnd: string;
    vehicleCount: number;
    ingestMode: string;
    kmTotal: number;
    kmTotalClosedWindow: number;
    currentDayKmProvisional: number;
    currentDayIsProvisional: boolean;
  };
  feed: FeedState;
  dataQuality: DataQuality;
  rules: DashboardRules;
  dms: DmsSnapshot;
  recentEvents: RecentEvent[];
  deviationByVehicle: Record<string, number>;
  reports: ReportFile[];
}

export interface FeedPollPayload {
  company_slug: string;
  company_name: string;
  connection_state: string;
  feed_status: FeedState["status"];
  feed_label: string;
  last_cycle_received_at: string | null;
  last_event_observed_at: string | null;
  last_error: string | null;
  new_cycle_available: boolean;
  anomaly_count_24h: number;
}

export interface AdminIngestionStatus {
  mode: string;
  connection_state: string;
  last_cycle_received_at: string | null;
  last_event_observed_at: string | null;
  last_alarm_at: string | null;
  last_status_at: string | null;
  last_device_sync_at: string | null;
  last_error: string | null;
  anomaly_count_24h: number;
}

export interface CoverageSummary {
  total_vehicles: number;
  reporting_vehicles_24h: number;
  stale_vehicles: number;
  vehicles_with_snapshot_today: number;
  vehicles_with_alarm_24h: number;
}

export interface KmSummary {
  total_window_km: number;
  closed_window_km: number;
  current_day_km_provisional: number;
  current_day_label: string;
}

export interface ReportsSummary {
  available_reports: number;
  latest_report_year: number | null;
  latest_report_month: number | null;
}

export interface AdminOverview {
  company_slug: string;
  company_name: string;
  ingest_mode: string;
  feed: FeedState;
  coverage: CoverageSummary;
  km: KmSummary;
  reports: ReportsSummary;
  anomaly_count_24h: number;
  active_notes: DataQualityNote[];
}

export interface CompanyAssignment {
  company_slug: string;
  fleet_ids: string[];
  device_ids: string[];
  points_to_mock: boolean;
  visible_devices: number;
  visible_mock_devices: number;
  visible_real_devices: number;
  visible_snapshots: number;
  visible_mock_snapshots: number;
  visible_real_snapshots: number;
  visible_alarms: number;
  visible_mock_alarms: number;
  visible_real_alarms: number;
}

export interface FleetCandidate {
  fleet_id: string;
  fleet_name: string | null;
  total_devices: number;
  devices_with_status: number;
  devices_seen_24h: number;
  alarm_events_7d: number;
  latest_seen_at: string | null;
  latest_alarm_at: string | null;
  sample_plates: string[];
  selected: boolean;
}

export interface MockDataSummary {
  devices_total: number;
  devices_mock: number;
  devices_real: number;
  snapshots_total: number;
  snapshots_mock: number;
  snapshots_real: number;
  alarms_total: number;
  alarms_mock: number;
  alarms_real: number;
}

export interface UnclassifiedCode {
  subtype: string | null;
  event_code: string | null;
  count: number;
  sample_device_id: string | null;
  sample_plate: string | null;
}

export interface AdminLiveSetup {
  company_slug: string;
  company_name: string;
  assignment: CompanyAssignment;
  mock_data: MockDataSummary;
  fleet_candidates: FleetCandidate[];
  unclassified_codes: UnclassifiedCode[];
}

export interface MockDataPurgeResult {
  deleted_devices: number;
  deleted_snapshots: number;
  deleted_alarms: number;
  deleted_mileage_readings: number;
}

export interface RecentAudit {
  raw_events: number;
  grouped_episodes: number;
  visible_alerts: number;
  dismissed_alerts: number;
  suppressed_by_rule: number;
  visible_raw_events: number;
  non_dms_hidden: number;
  unmapped_hidden: number;
  future_rejected: number;
}

export interface AlarmAudit {
  accepted_total: number;
  visible_total: number;
  unclassified_total: number;
  mapping_sources: Record<string, number>;
  by_category: Record<string, number>;
  by_subtype: Array<{ subtype: string; count: number }>;
}

export interface AnomalyAudit {
  total: number;
  by_reason: Record<string, number>;
}

export interface AdminAudit {
  company_slug: string;
  company_name: string;
  range_start: string;
  range_end: string;
  alarms: AlarmAudit;
  anomalies: AnomalyAudit;
  recent_24h: RecentAudit;
}

export interface ReconciliationSummary {
  company_slug: string;
  company_name: string;
  window_type: "calendar_day_local" | "rolling_24h";
  range_start: string;
  range_end: string;
  raw_portal_equivalent: number;
  ingested_live: number;
  ingested_backfill: number;
  classified_dms: number;
  classified_non_dms: number;
  visible_episodes: number;
  visible_raw_events: number;
  suppressed_by_rule: number;
  rejected_temporal: number;
  unmapped: number;
  missing_local: number;
}

export interface ReconciliationDrilldownRow {
  guid: string;
  plate_no: string | null;
  device_id: string | null;
  raw_alarm_type: string | null;
  raw_tp: string | null;
  raw_event_code: string | null;
  observed_at: string | null;
  classification_status: string;
  visibility_status: string;
  source: string;
  category: string | null;
  subtype: string | null;
  reason: string;
  episode_guid: string | null;
  episode_title: string | null;
}

export interface KmQualitySummary {
  company_slug: string;
  company_name: string;
  vehicles_with_valid_day_km: number;
  vehicles_with_invalid_day_km: number;
  vehicles_with_total_regression: number;
  current_day_km_source: string;
  repaired_rows: number;
  sample_invalid_vehicles: string[];
}

export interface AdminVehicle {
  device_id: string;
  plate_no: string | null;
  fleet_id: string | null;
  fleet_name: string | null;
  device_name: string | null;
  driver_name: string | null;
  last_received_at: string | null;
  last_seen_at: string | null;
  last_alarm_at: string | null;
  last_total_km: number | null;
  last_day_km: number | null;
  last_snapshot_total_km: number | null;
  last_snapshot_day_km: number | null;
  last_snapshot_at: string | null;
  feed_status: FeedState["status"];
  record_source: string | null;
}

export interface IngestionAnomaly {
  id: number;
  source_type: string;
  device_id: string | null;
  company_slug: string | null;
  received_at: string;
  raw_event_time: string | null;
  reason: string;
  payload_json: string;
}
