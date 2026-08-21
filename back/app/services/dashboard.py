from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
import hashlib
import socket
import json
from statistics import mean
from time import monotonic
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import case, func, or_, select
from sqlalchemy.orm import load_only

from app.core.catalog import CATEGORY_META, CATEGORY_ORDER
from app.core.time import as_timezone, ensure_utc, parse_timestamp, utc_now
from app.models import AlarmEvent, AlarmEventAudit, AlarmHarvestRun, CatchupCursor, CompanyHistoricalRebuildJob, DailyMileageSnapshot, DeviceRecord, HowenAlarmRaw, IngestState, IngestionAnomaly, MileageReading, PublishedDashboardSnapshot, ReconciliationJob, ReconciliationJobDevice, ReconciliationReview, ReportAsset
from app.schemas import (
    AdminLiveSetupView,
    AdminAuditView,
    AdminIngestionStatusView,
    AdminOverviewView,
    AdminVehicleView,
    AlarmAuditView,
    AnomalyAuditView,
    CompanyAssignmentView,
    CompanyConfig,
    CoverageSummaryView,
    FleetCandidateView,
    FeedSocketPayload,
    FeedState,
    KmQualitySummary,
    KmRepairRequest,
    KmSummaryView,
    MockDataPurgeResult,
    MockDataSummaryView,
    OperationalRecencyView,
    ReconciliationDrilldownRow,
    ReconciliationJobView,
    ReconciliationReviewBulkDecisionResponse,
    ReconciliationReviewDecisionRequest,
    ReconciliationReviewItemView,
    ReconciliationRunRequest,
    ReconciliationRunResponse,
    ReconciliationSummary,
    RecentAuditView,
    RawAlarmDiagnosticView,
    ReportFileView,
    ReportsSummaryView,
    ReconciliationReviewListView,
    UnclassifiedCodeView,
)
from app.services.company_registry import CompanyRegistry, normalize_plate_label
from app.services.howen import HowenClient, HowenRateLimitError

# `admin_backfill` is a legacy production source label from earlier manual
# reconciliation runs. We still count it while the historical-cut pipeline
# finishes migrating existing datasets to canonical `harvest/backfill` sources.
ACTIVE_EVENT_SOURCES = ("harvest", "live", "backfill", "catchup", "admin_backfill")
ACTIVE_SNAPSHOT_SOURCES = ("live", "harvest", "backfill", "catchup")
ACTIVE_MILEAGE_SOURCES = ("status", "live", "backfill")
MOCK_DEVICE_PREFIX = "DEV-"
MOCK_FLEET_IDS = {"cotaba-main"}
PORTAL_FETCH_DELAY_SECONDS = 0.35
PORTAL_RATE_LIMIT_BACKOFF_SECONDS = (1.5, 3.0, 5.0)
RECONCILIATION_FETCH_TIMEOUT_SECONDS = 180.0
RECONCILIATION_DEVICE_TIMEOUT_SECONDS = 45.0
ADMIN_OVERVIEW_CACHE_SECONDS = 15.0
ADMIN_COMPANY_CATALOG_CACHE_SECONDS = 10.0
ADMIN_AUDIT_CACHE_SECONDS = 20.0
KM_QUALITY_CACHE_SECONDS = 20.0


class DashboardService:
    def __init__(self, *, session_factory: Any, registry: CompanyRegistry, settings: Any) -> None:
        self.session_factory = session_factory
        self.registry = registry
        self.settings = settings
        self.howen = HowenClient(settings=settings, registry=registry)
        self._reconciliation_tasks: dict[str, asyncio.Task[None]] = {}
        self._reconciliation_locks: dict[str, asyncio.Lock] = {}
        self._admin_overview_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._admin_company_catalog_cache: tuple[float, dict[str, Any]] | None = None
        self._admin_audit_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._km_quality_cache: dict[str, tuple[float, dict[str, Any]]] = {}

    def clear_runtime_caches(self) -> None:
        self._admin_overview_cache.clear()
        self._admin_company_catalog_cache = None
        self._admin_audit_cache.clear()
        self._km_quality_cache.clear()

    def _latest_activation_rebuilds(self, session: Any) -> dict[str, CompanyHistoricalRebuildJob]:
        latest_rebuilds: dict[str, CompanyHistoricalRebuildJob] = {}
        for row in session.scalars(
            select(CompanyHistoricalRebuildJob)
            .where(CompanyHistoricalRebuildJob.purpose == "activation_bootstrap")
            .order_by(CompanyHistoricalRebuildJob.created_at.desc(), CompanyHistoricalRebuildJob.id.desc())
        ):
            latest_rebuilds.setdefault(row.company_slug, row)
        return latest_rebuilds

    @staticmethod
    def _company_ready_for_selector(
        *,
        publication: PublishedDashboardSnapshot | None,
        rebuild: CompanyHistoricalRebuildJob | None,
    ) -> bool:
        rebuild_status = rebuild.status if rebuild else "idle"
        return bool(
            publication
            and publication.snapshot_json
            and rebuild_status not in {"queued", "running", "failed"}
        )

    def _load_published_snapshot(self, company_slug: str) -> dict[str, Any] | None:
        with self.session_factory() as session:
            publication = session.get(PublishedDashboardSnapshot, company_slug)
            if not publication or not publication.snapshot_json:
                return None
            try:
                payload = json.loads(publication.snapshot_json)
            except json.JSONDecodeError:
                return None
            company = self.registry.get(company_slug)
            state = session.get(IngestState, "global")
            anomalies = list(
                session.scalars(
                    select(IngestionAnomaly)
                    .where(
                        IngestionAnomaly.company_slug == company_slug,
                        IngestionAnomaly.received_at >= utc_now() - timedelta(hours=24),
                    )
                    .order_by(IngestionAnomaly.received_at.desc())
                )
            )
        if state:
            state.last_message_at = ensure_utc(state.last_message_at)
            state.last_cycle_received_at = ensure_utc(state.last_cycle_received_at)
            state.last_event_observed_at = ensure_utc(state.last_event_observed_at)
            state.last_alarm_at = ensure_utc(state.last_alarm_at)
            state.last_status_at = ensure_utc(state.last_status_at)
            state.last_live_alarm_message_at = ensure_utc(state.last_live_alarm_message_at)
            state.last_live_dms_at = ensure_utc(state.last_live_dms_at)
            state.last_live_unmapped_at = ensure_utc(state.last_live_unmapped_at)
        payload["feed"] = self._build_feed_state(
            state=state,
            company=company,
            now_local=utc_now().astimezone(ZoneInfo(company.timezone or self.settings.default_timezone)),
        )
        payload.setdefault("dataQuality", {})
        payload["dataQuality"]["anomaly_count_24h"] = len(anomalies)
        payload["dataQuality"]["last_anomaly_at"] = anomalies[0].received_at.isoformat() if anomalies else None
        return self._decorate_snapshot_with_publication(
            payload,
            publication=publication,
            fallback_cut_at=ensure_utc(publication.published_cut_at),
            last_status_message_at=ensure_utc(state.last_message_at) if state else ensure_utc(publication.last_status_message_at),
            last_status_observed_at=ensure_utc(state.last_status_at) if state else ensure_utc(publication.last_status_observed_at),
        )

    def materialize_snapshot(
        self,
        company_slug: str,
        *,
        cut_at: datetime,
        cut_status: str = "succeeded",
        last_error: str | None = None,
    ) -> dict[str, Any]:
        cut_at = ensure_utc(cut_at) or utc_now()
        payload = self.build_snapshot(
            company_slug,
            force_recompute=True,
            published_cut_at=cut_at,
        )
        next_cut_at = _next_cut_boundary(cut_at, self.settings.harvest_cut_interval_minutes)
        now_utc = utc_now()
        with self.session_factory() as session:
            ingest_state = session.get(IngestState, "global")
            publication = session.get(PublishedDashboardSnapshot, company_slug) or PublishedDashboardSnapshot(company_slug=company_slug)
            publication.published_cut_at = cut_at
            publication.next_cut_at = next_cut_at
            publication.window_start = cut_at - timedelta(minutes=self.settings.harvest_cut_interval_minutes)
            publication.window_end = cut_at
            publication.cut_status = cut_status
            publication.last_completed_harvest_at = now_utc if cut_status == "succeeded" else publication.last_completed_harvest_at
            publication.last_status_message_at = ensure_utc(ingest_state.last_message_at) if ingest_state else publication.last_status_message_at
            publication.last_status_observed_at = ensure_utc(ingest_state.last_status_at) if ingest_state else publication.last_status_observed_at
            publication.last_dms_published_at = parse_timestamp(payload["meta"].get("lastDmsEventAt")) if payload["meta"].get("lastDmsEventAt") else None
            publication.last_error = last_error
            payload = self._decorate_snapshot_with_publication(
                payload,
                publication=publication,
                fallback_cut_at=cut_at,
                last_status_message_at=publication.last_status_message_at,
                last_status_observed_at=publication.last_status_observed_at,
            )
            publication.snapshot_json = json.dumps(payload, ensure_ascii=True)
            session.add(publication)
            session.commit()
        self.clear_runtime_caches()
        return payload

    def build_snapshot(
        self,
        company_slug: str,
        *,
        force_recompute: bool = False,
        published_cut_at: datetime | None = None,
    ) -> dict[str, Any]:
        if published_cut_at is None and not force_recompute:
            published = self._load_published_snapshot(company_slug)
            if published:
                return published

        company = self.registry.get(company_slug)
        tz = ZoneInfo(company.timezone or self.settings.default_timezone)
        reference_utc = ensure_utc(published_cut_at) or utc_now()
        generated_at = utc_now()
        now_local = reference_utc.astimezone(tz)
        cutoff = reference_utc - timedelta(days=self.settings.live_retention_days)
        recent_cutoff = reference_utc - timedelta(hours=24)
        anomaly_cutoff = reference_utc - timedelta(hours=24)
        alarm_membership = _company_membership_clause(AlarmEvent, company)
        snapshot_membership = _company_membership_clause(DailyMileageSnapshot, company)
        device_membership = _company_membership_clause(DeviceRecord, company)

        with self.session_factory() as session:
            publication = session.get(PublishedDashboardSnapshot, company_slug)
            events = list(
                session.scalars(
                    select(AlarmEvent)
                    .options(
                        load_only(
                            AlarmEvent.guid,
                            AlarmEvent.provider_event_key,
                            AlarmEvent.device_id,
                            AlarmEvent.plate_no,
                            AlarmEvent.company_slug,
                            AlarmEvent.fleet_id,
                            AlarmEvent.driver_name,
                            AlarmEvent.category,
                            AlarmEvent.subtype,
                            AlarmEvent.classification_status,
                            AlarmEvent.raw_alarm_type,
                            AlarmEvent.raw_tp,
                            AlarmEvent.raw_event_code,
                            AlarmEvent.raw_event_time,
                            AlarmEvent.occurred_at,
                            AlarmEvent.start_at,
                            AlarmEvent.end_at,
                            AlarmEvent.latitude,
                            AlarmEvent.longitude,
                            AlarmEvent.total_mileage_km,
                        )
                    )
                    .where(
                        alarm_membership,
                        AlarmEvent.source.in_(ACTIVE_EVENT_SOURCES),
                        AlarmEvent.occurred_at >= cutoff,
                        AlarmEvent.occurred_at <= reference_utc,
                    )
                    .order_by(AlarmEvent.occurred_at)
                )
            )
            daily_snapshots = list(
                session.scalars(
                    select(DailyMileageSnapshot)
                    .options(
                        load_only(
                            DailyMileageSnapshot.device_id,
                            DailyMileageSnapshot.plate_no,
                            DailyMileageSnapshot.company_slug,
                            DailyMileageSnapshot.fleet_id,
                            DailyMileageSnapshot.snapshot_date,
                            DailyMileageSnapshot.total_km,
                            DailyMileageSnapshot.day_km,
                            DailyMileageSnapshot.observed_at,
                        )
                    )
                    .where(
                        snapshot_membership,
                        DailyMileageSnapshot.source.in_(ACTIVE_SNAPSHOT_SOURCES),
                        DailyMileageSnapshot.observed_at >= cutoff,
                        DailyMileageSnapshot.observed_at <= reference_utc,
                    )
                    .order_by(DailyMileageSnapshot.observed_at)
                )
            )
            devices = list(
                session.scalars(
                    select(DeviceRecord)
                    .options(
                        load_only(
                            DeviceRecord.device_id,
                            DeviceRecord.plate_no,
                            DeviceRecord.company_slug,
                            DeviceRecord.fleet_id,
                            DeviceRecord.last_seen_at,
                            DeviceRecord.last_received_at,
                            DeviceRecord.last_total_km,
                            DeviceRecord.last_day_km,
                        )
                    )
                    .where(device_membership, DeviceRecord.record_source == "live")
                    .order_by(DeviceRecord.device_id)
                )
            )
            legacy_daily_km = _load_legacy_daily_km(
                session,
                company=company,
                cutoff=cutoff,
                reference_utc=reference_utc,
            )
            legacy_mileages: list[MileageReading] = []
            reports = list(
                session.scalars(
                    select(ReportAsset)
                    .where(ReportAsset.company_slug == company_slug)
                    .order_by(ReportAsset.year.desc(), ReportAsset.month.desc())
                )
            )
            anomalies = list(
                session.scalars(
                    select(IngestionAnomaly)
                    .options(load_only(IngestionAnomaly.received_at))
                    .where(
                        IngestionAnomaly.received_at >= anomaly_cutoff,
                        IngestionAnomaly.received_at <= reference_utc,
                        IngestionAnomaly.company_slug == company_slug,
                    )
                    .order_by(IngestionAnomaly.received_at.desc())
                )
            )
            state = session.get(IngestState, "global")
            review_status_by_guid = _load_review_status_map(session, company_slug)

        for event in events:
            event.occurred_at = ensure_utc(event.occurred_at) or event.occurred_at
            event.start_at = ensure_utc(event.start_at)
            event.end_at = ensure_utc(event.end_at)

        for snapshot in daily_snapshots:
            snapshot.observed_at = ensure_utc(snapshot.observed_at) or snapshot.observed_at

        for reading in legacy_mileages:
            reading.recorded_at = ensure_utc(reading.recorded_at) or reading.recorded_at

        for device in devices:
            device.last_received_at = ensure_utc(device.last_received_at)
            device.last_seen_at = ensure_utc(device.last_seen_at)

        for report in reports:
            report.uploaded_at = ensure_utc(report.uploaded_at) or report.uploaded_at

        if state:
            state.last_message_at = ensure_utc(state.last_message_at)
            state.last_cycle_received_at = ensure_utc(state.last_cycle_received_at)
            state.last_event_observed_at = ensure_utc(state.last_event_observed_at)
            state.last_status_at = ensure_utc(state.last_status_at)
            state.last_alarm_at = ensure_utc(state.last_alarm_at)
            state.last_live_alarm_message_at = ensure_utc(state.last_live_alarm_message_at)
            state.last_live_dms_at = ensure_utc(state.last_live_dms_at)
            state.last_live_unmapped_at = ensure_utc(state.last_live_unmapped_at)
            state.last_device_sync_at = ensure_utc(state.last_device_sync_at)
            state.last_anomaly_at = ensure_utc(state.last_anomaly_at)

        company_events = [
            event
            for event in events
            if self.registry.device_belongs(company, event.device_id, event.fleet_id)
            and event.classification_status == "classified_dms"
            and self.registry.category_allowed(company, event.category)
            and review_status_by_guid.get(event.guid) != "discarded"
        ]
        company_snapshots = [
            snapshot
            for snapshot in daily_snapshots
            if self.registry.device_belongs(company, snapshot.device_id, snapshot.fleet_id)
        ]
        company_legacy = [
            reading
            for reading in legacy_mileages
            if self.registry.device_belongs(company, reading.device_id, reading.fleet_id)
        ]
        company_devices = [
            device
            for device in devices
            if self.registry.device_belongs(company, device.device_id, device.fleet_id)
        ]

        canonical_plate_by_device = {
            device.device_id: self.registry.canonical_plate(device.device_id, device.plate_no)
            for device in company_devices
        }
        canonical_legacy_daily_km = [
            (
                canonical_plate_by_device.get(device_id)
                or self.registry.canonical_plate(device_id, plate_no)
                or device_id,
                day_key,
                day_km,
            )
            for device_id, plate_no, day_key, day_km in legacy_daily_km
        ]
        for event in company_events:
            event.plate_no = self.registry.canonical_plate(
                event.device_id,
                canonical_plate_by_device.get(event.device_id),
                event.plate_no,
            )
        for snapshot in company_snapshots:
            snapshot.plate_no = self.registry.canonical_plate(
                snapshot.device_id,
                canonical_plate_by_device.get(snapshot.device_id),
                snapshot.plate_no,
            )
        for reading in company_legacy:
            reading.plate_no = self.registry.canonical_plate(
                reading.device_id,
                canonical_plate_by_device.get(reading.device_id),
                reading.plate_no,
            )

        dates_30 = _date_window(now_local.date(), 30)
        dates_7 = dates_30[-7:]
        dates_30_set = set(dates_30)
        dates_7_set = set(dates_7)
        latest_day = dates_30[-1]
        closed_days = dates_30[:-1]
        daily_km_by_vehicle, fleet_km_by_date = _build_daily_km(
            company_snapshots,
            company_legacy,
            company_events,
            tz,
            legacy_daily_km=canonical_legacy_daily_km,
        )
        _merge_current_day_from_device_state(company_devices, daily_km_by_vehicle, fleet_km_by_date, latest_day, tz)
        episode_analysis = _build_recent_episode_analysis(
            company_events,
            company,
            tz,
            daily_km_by_vehicle,
            review_status_by_guid=review_status_by_guid,
            fleet_vehicle_count=len(company_devices),
        )
        event_visibility = episode_analysis["guid_status"]
        self._persist_suppressed_rule_reviews(
            company=company,
            events=company_events,
            guid_status=event_visibility,
        )
        company_events = [
            event
            for event in company_events
            if event_visibility.get(event.guid, {}).get("visibility_status")
            in {"visible_episode", "fused_in_episode"}
        ]
        recent_events = [
            _serialize_event(event, tz, event_visibility.get(event.guid))
            for event in company_events
            if event.occurred_at >= recent_cutoff
        ]
        last_dms_event_at = company_events[-1].occurred_at.isoformat() if company_events else None
        current_day_km_provisional = round(fleet_km_by_date.get(latest_day, 0.0), 1)
        km_total_closed_window = round(sum(fleet_km_by_date.get(day_key, 0.0) for day_key in closed_days), 1)
        km_coverage_dates = [day_key for day_key in dates_30 if day_key in fleet_km_by_date]

        event_dates = defaultdict(list)
        events_by_vehicle = defaultdict(list)
        events_by_vehicle_day = defaultdict(Counter)
        for event in company_events:
            local_dt = event.occurred_at.astimezone(tz)
            day_key = local_dt.date()
            plate = event.plate_no or event.device_id
            event_dates[day_key].append(event)
            events_by_vehicle[plate].append(event)
            events_by_vehicle_day[plate][day_key] += 1

        category_order = [category for category in CATEGORY_ORDER if category in set(company.allowed_categories or CATEGORY_ORDER)]
        if not category_order:
            category_order = list(CATEGORY_ORDER)

        serie_cat = {category: [0 for _ in dates_30] for category in category_order}
        heat = {category: [0 for _ in range(24)] for category in category_order}
        dist_counter: Counter[tuple[str, str]] = Counter()
        for day_index, day_key in enumerate(dates_30):
            for event in event_dates.get(day_key, []):
                if event.category in serie_cat:
                    serie_cat[event.category][day_index] += 1
                    heat[event.category][event.occurred_at.astimezone(tz).hour] += 1
                subtype_label = event.subtype or event.category
                dist_counter[(subtype_label, event.category)] += 1

        today_events = event_dates.get(latest_day, [])
        today_total = len(today_events)
        today_by_category = Counter(event.category for event in today_events)
        today_vehicle_counts = Counter((event.plate_no or event.device_id) for event in today_events)
        baseline_dates = _date_window(now_local.date() - timedelta(days=1), 30)
        baselines = _build_baselines(
            baseline_dates=baseline_dates,
            daily_km_by_vehicle=daily_km_by_vehicle,
            events_by_vehicle_day=events_by_vehicle_day,
        )
        fleet_daily_baseline = round(mean([len(event_dates.get(day_key, [])) for day_key in baseline_dates]), 2) if baseline_dates else 0.0
        fleet_daily_delta_pct = (
            round(((today_total - fleet_daily_baseline) / fleet_daily_baseline) * 100, 1) if fleet_daily_baseline else None
        )

        table_rows = []
        deviation_by_vehicle: dict[str, float] = {}
        device_labels = {device.plate_no or device.device_id for device in company_devices}
        plates = sorted(device_labels | set(events_by_vehicle) | set(daily_km_by_vehicle))
        for plate in plates:
            plate_events = [event for event in company_events if (event.plate_no or event.device_id) == plate]
            counts_30 = Counter(
                event.category for event in plate_events if event.occurred_at.astimezone(tz).date() in dates_30_set
            )
            total_30 = sum(counts_30.values())
            km_30 = round(sum(daily_km_by_vehicle.get(plate, {}).get(day_key, 0.0) for day_key in dates_30), 1)
            km_coverage_complete = all(day_key in daily_km_by_vehicle.get(plate, {}) for day_key in dates_30)
            nocturnal = sum(
                1
                for event in plate_events
                if event.occurred_at.astimezone(tz).date() in dates_30_set
                and _is_night(event.occurred_at.astimezone(tz).hour, company.rules.night_window_start, company.rules.night_window_end)
            )
            risk_score = sum(CATEGORY_META.get(category, {"weight": 0})["weight"] * count for category, count in counts_30.items())
            baseline = round(baselines.get(plate, 0.0), 2)
            plate_today_total = today_vehicle_counts.get(plate, 0)
            deviation = round((plate_today_total / baseline), 2) if baseline else float(plate_today_total or 1.0)
            deviation_by_vehicle[plate] = deviation if deviation else 1.0
            table_rows.append(
                {
                    "placa": plate,
                    "total": total_30,
                    "km": km_30,
                    "por100km": _rate(total_30, km_30) if km_coverage_complete else None,
                    "riesgo100km": _rate(risk_score, km_30) if km_coverage_complete else None,
                    "nocturno": nocturnal,
                    "cats": {category: counts_30.get(category, 0) for category in category_order},
                    "baseline": baseline,
                    "spike": baseline > 0 and plate_today_total >= baseline * company.rules.spike_threshold_multiplier,
                }
            )

        table_rows.sort(key=lambda row: (row["riesgo100km"] is None, -(row["riesgo100km"] or 0), row["placa"]))
        weekly_top = [row["placa"] for row in table_rows]
        semana = {
            "veh": weekly_top,
            "cat_veh": {
                category: [
                    sum(
                        1
                        for event in events_by_vehicle.get(plate, [])
                        if event.category == category and event.occurred_at.astimezone(tz).date() in dates_7_set
                    )
                    for plate in weekly_top
                ]
                for category in category_order
            },
            "fechas": [day_key.isoformat() for day_key in dates_7],
            "linea_veh": {
                plate: [events_by_vehicle_day.get(plate, Counter()).get(day_key, 0) for day_key in dates_7]
                for plate in weekly_top
            },
            "total": sum(len(event_dates.get(day_key, [])) for day_key in dates_7),
        }

        total_30 = sum(len(event_dates.get(day_key, [])) for day_key in dates_30)
        total_km_30 = round(sum(fleet_km_by_date.get(day_key, 0.0) for day_key in dates_30), 1)
        severity_totals = Counter(
            CATEGORY_META.get(event.category, {"severity": "medio"})["severity"]
            for event in company_events
            if event.occurred_at.astimezone(tz).date() in dates_30_set
        )
        nocturnal_30 = sum(
            1
            for event in company_events
            if event.occurred_at.astimezone(tz).date() in dates_30_set
            and _is_night(event.occurred_at.astimezone(tz).hour, company.rules.night_window_start, company.rules.night_window_end)
        )
        visible_reports = [report for report in reports if (report.year, report.month) < (now_local.year, now_local.month)]

        payload = {
            "meta": {
                "companySlug": company.slug,
                "companyName": company.name,
                "customer": company.customer,
                "brand": company.brand.model_dump(),
                "generatedAt": generated_at.isoformat(),
                "timezone": company.timezone,
                "rangeStart": dates_30[0].isoformat(),
                "rangeEnd": latest_day.isoformat(),
                "vehicleCount": len({device.device_id for device in company_devices}),
                "ingestMode": "live",
                "kmTotal": total_km_30,
                "kmTotalClosedWindow": km_total_closed_window,
                "currentDayKmProvisional": current_day_km_provisional,
                "currentDayIsProvisional": True,
                "kmCoverageDays": len(km_coverage_dates),
                "kmWindowDays": len(dates_30),
                "kmCoverageStart": km_coverage_dates[0].isoformat() if km_coverage_dates else None,
                "kmDataComplete": len(km_coverage_dates) == len(dates_30),
                "lastDmsEventAt": last_dms_event_at,
                "weekWindowStart": dates_7[0].isoformat(),
                "weekWindowEnd": dates_7[-1].isoformat(),
                "weekWindowMode": "calendar_local",
            },
            "feed": self._build_feed_state(state=state, company=company, now_local=now_local),
            "dataQuality": {
                "active_notes": [
                    note.model_dump(mode="json")
                    for note in self.registry.active_quality_notes(company, range_start=dates_30[0], range_end=latest_day)
                ],
                "anomaly_count_24h": len(anomalies),
                "last_anomaly_at": anomalies[0].received_at.isoformat() if anomalies else None,
            },
            "rules": company.rules.model_dump(),
            "dms": {
                "ultimo": {
                    "total": today_total,
                    "baseline_promedio": fleet_daily_baseline,
                    "delta_pct": fleet_daily_delta_pct,
                    "por_cat": {category: today_by_category.get(category, 0) for category in category_order},
                    "por_vehiculo": [
                        {
                            "placa": plate,
                            "total": total,
                            "baseline": round(baselines.get(plate, 0.0), 2),
                            "spike": baselines.get(plate, 0.0) > 0
                            and total >= baselines.get(plate, 0.0) * company.rules.spike_threshold_multiplier,
                        }
                        for plate, total in today_vehicle_counts.most_common(8)
                    ],
                },
                "kpis": {
                    "total": total_30,
                    "critico": severity_totals.get("critico", 0),
                    "alto": severity_totals.get("alto", 0),
                    "medio": severity_totals.get("medio", 0),
                    "km": total_km_30,
                    "por100km": _rate(total_30, total_km_30) if len(km_coverage_dates) == len(dates_30) else None,
                    "nocturno_pct": round((nocturnal_30 / total_30) * 100, 1) if total_30 else 0,
                    "rango": f"{dates_30[0].isoformat()} a {latest_day.isoformat()}",
                },
                "semana": semana,
                "fechas": [day_key.isoformat() for day_key in dates_30],
                "serie_cat": serie_cat,
                "km_dia": [
                    round(fleet_km_by_date[day_key], 1) if day_key in fleet_km_by_date else None
                    for day_key in dates_30
                ],
                "dist_tipo": [
                    {"tipo": subtype, "cat": category, "n": count}
                    for (subtype, category), count in dist_counter.most_common(20)
                ],
                "cat_order": category_order,
                "heat": heat,
                "tabla": table_rows,
            },
            "recentEvents": recent_events,
            "deviationByVehicle": deviation_by_vehicle,
            "reports": [
                ReportFileView(
                    year=report.year,
                    month=report.month,
                    original_name=report.original_name,
                    size_bytes=report.size_bytes,
                    uploaded_at=report.uploaded_at,
                    download_url=f"{self.settings.api_prefix}/reports/{report.year}/{report.month}",
                ).model_dump(mode="json")
                for report in visible_reports
            ],
        }
        return self._decorate_snapshot_with_publication(
            payload,
            publication=publication,
            fallback_cut_at=ensure_utc(published_cut_at),
            last_status_message_at=ensure_utc(state.last_message_at) if state else None,
            last_status_observed_at=ensure_utc(state.last_status_at) if state else None,
        )

    def _decorate_snapshot_with_publication(
        self,
        payload: dict[str, Any],
        *,
        publication: PublishedDashboardSnapshot | None,
        fallback_cut_at: datetime | None,
        last_status_message_at: datetime | None,
        last_status_observed_at: datetime | None,
    ) -> dict[str, Any]:
        meta = payload.setdefault("meta", {})
        published_cut_at = ensure_utc(publication.published_cut_at) if publication and publication.published_cut_at else fallback_cut_at
        next_cut_at = ensure_utc(publication.next_cut_at) if publication and publication.next_cut_at else _next_cut_boundary(
            published_cut_at or utc_now(),
            self.settings.harvest_cut_interval_minutes,
        )
        meta["publishedCutAt"] = published_cut_at.isoformat() if published_cut_at else None
        meta["nextCutAt"] = next_cut_at.isoformat() if next_cut_at else None
        meta["cutStatus"] = publication.cut_status if publication else ("succeeded" if published_cut_at else "pending")
        meta["lastCompletedHarvestAt"] = (
            ensure_utc(publication.last_completed_harvest_at).isoformat()
            if publication and publication.last_completed_harvest_at
            else None
        )
        meta["lastStatusMessageAt"] = last_status_message_at.isoformat() if last_status_message_at else None
        meta["lastStatusObservedAt"] = last_status_observed_at.isoformat() if last_status_observed_at else None
        meta["lastDmsPublishedAt"] = (
            ensure_utc(publication.last_dms_published_at).isoformat()
            if publication and publication.last_dms_published_at
            else meta.get("lastDmsEventAt")
        )
        return payload

    def build_feed_poll(self, company_slug: str, known_cycle_at: datetime | None = None) -> dict[str, Any]:
        company = self.registry.get(company_slug)
        now_local = utc_now().astimezone(ZoneInfo(company.timezone or self.settings.default_timezone))
        with self.session_factory() as session:
            state = session.get(IngestState, "global")
            anomalies = list(
                session.scalars(
                    select(IngestionAnomaly)
                    .where(
                        IngestionAnomaly.company_slug == company_slug,
                        IngestionAnomaly.received_at >= utc_now() - timedelta(hours=24),
                    )
                    .order_by(IngestionAnomaly.received_at.desc())
                )
            )
        if state:
            state.last_message_at = ensure_utc(state.last_message_at)
            state.last_cycle_received_at = ensure_utc(state.last_cycle_received_at)
            state.last_event_observed_at = ensure_utc(state.last_event_observed_at)
            state.last_alarm_at = ensure_utc(state.last_alarm_at)
            state.last_status_at = ensure_utc(state.last_status_at)
            state.last_live_alarm_message_at = ensure_utc(state.last_live_alarm_message_at)
            state.last_live_dms_at = ensure_utc(state.last_live_dms_at)
            state.last_live_unmapped_at = ensure_utc(state.last_live_unmapped_at)
        feed = self._build_feed_state(state=state, company=company, now_local=now_local)
        new_cycle_available = bool(
            known_cycle_at and state and state.last_cycle_received_at and state.last_cycle_received_at > ensure_utc(known_cycle_at)
        )
        return FeedSocketPayload(
            company_slug=company.slug,
            company_name=company.name,
            connection_state=feed["connection_state"],
            feed_status=feed["status"],
            feed_label=feed["label"],
            last_cycle_received_at=feed.get("last_cycle_received_at"),
            last_event_observed_at=feed.get("last_event_observed_at"),
            last_error=feed.get("last_error"),
            new_cycle_available=new_cycle_available,
            anomaly_count_24h=len(anomalies),
        ).model_dump(mode="json")

    def build_admin_ingestion_status(self, *, company_slug: str | None = None) -> dict[str, Any]:
        now = utc_now()
        anomaly_cutoff = now - timedelta(hours=24)
        company = self.registry.get(company_slug) if company_slug else None
        with self.session_factory() as session:
            state = session.get(IngestState, "global")
            catchup_cursor = session.get(CatchupCursor, company_slug) if company_slug else None
            anomaly_query = session.query(IngestionAnomaly).filter(IngestionAnomaly.received_at >= anomaly_cutoff)
            if company_slug:
                anomaly_query = anomaly_query.filter(IngestionAnomaly.company_slug == company_slug)
            anomalies = list(anomaly_query)
            raw_metrics = self._load_raw_alarm_metrics(session, company=company, company_slug=company_slug, cutoff=anomaly_cutoff)
            if company:
                operational_recency = self._build_operational_recency(session, company=company, reference_at=now)
            else:
                operational_recency = self._combine_operational_recency(
                    [self._build_operational_recency(session, company=item, reference_at=now) for item in self.registry.all()]
                )
        if state:
            state.last_cycle_received_at = ensure_utc(state.last_cycle_received_at)
            state.last_event_observed_at = ensure_utc(state.last_event_observed_at)
            state.last_status_at = ensure_utc(state.last_status_at)
            state.last_alarm_at = ensure_utc(state.last_alarm_at)
            state.last_live_alarm_message_at = ensure_utc(state.last_live_alarm_message_at)
            state.last_live_dms_at = ensure_utc(state.last_live_dms_at)
            state.last_live_unmapped_at = ensure_utc(state.last_live_unmapped_at)
            state.last_device_sync_at = ensure_utc(state.last_device_sync_at)
            state.maintenance_started_at = ensure_utc(state.maintenance_started_at)
        if catchup_cursor:
            catchup_cursor.last_successful_catchup_cursor_at = ensure_utc(catchup_cursor.last_successful_catchup_cursor_at)
            catchup_cursor.last_successful_catchup_observed_at = ensure_utc(catchup_cursor.last_successful_catchup_observed_at)
            catchup_cursor.pending_range_start_at = ensure_utc(catchup_cursor.pending_range_start_at)
            catchup_cursor.pending_range_end_at = ensure_utc(catchup_cursor.pending_range_end_at)
            catchup_cursor.next_retry_at = ensure_utc(catchup_cursor.next_retry_at)
            catchup_cursor.last_attempt_at = ensure_utc(catchup_cursor.last_attempt_at)
        payload = AdminIngestionStatusView(
            mode=state.mode if state else "live",
            connection_state=state.connection_state if state else "idle",
            maintenance_mode=bool(state.maintenance_mode) if state else False,
            maintenance_reason=state.maintenance_reason if state else None,
            maintenance_started_at=state.maintenance_started_at if state else None,
            last_cycle_received_at=state.last_cycle_received_at if state else None,
            last_event_observed_at=state.last_event_observed_at if state else None,
            last_alarm_at=state.last_alarm_at if state else None,
            last_status_at=state.last_status_at if state else None,
            last_live_alarm_message_at=state.last_live_alarm_message_at if state else None,
            last_live_dms_at=state.last_live_dms_at if state else None,
            last_live_unmapped_at=state.last_live_unmapped_at if state else None,
            last_device_sync_at=state.last_device_sync_at if state else None,
            last_error=state.last_error if state else None,
            anomaly_count_24h=len(anomalies),
            live_alarm_count_24h=raw_metrics["live_alarm_count_24h"],
            live_dms_count_24h=raw_metrics["live_dms_count_24h"],
            raw_dms_count_24h=raw_metrics["raw_dms_count_24h"],
            backfill_dms_count_24h=raw_metrics["backfill_dms_count_24h"],
            catchup_dms_count_24h=raw_metrics["catchup_dms_count_24h"],
            live_unmapped_count_24h=raw_metrics["live_unmapped_count_24h"],
            non_dms_count_24h=raw_metrics["non_dms_count_24h"],
            live_non_dms_count_24h=raw_metrics["live_non_dms_count_24h"],
            future_rejected_count_24h=raw_metrics["future_rejected_count_24h"],
            live_future_rejected_count_24h=raw_metrics["live_future_rejected_count_24h"],
            catchup_failures_24h=sum(1 for anomaly in anomalies if anomaly.reason.startswith("catchup_failed:")),
            last_successful_catchup_cursor_at=catchup_cursor.last_successful_catchup_cursor_at if catchup_cursor else None,
            last_successful_catchup_observed_at=catchup_cursor.last_successful_catchup_observed_at if catchup_cursor else None,
            pending_range_start_at=catchup_cursor.pending_range_start_at if catchup_cursor else None,
            pending_range_end_at=catchup_cursor.pending_range_end_at if catchup_cursor else None,
            next_catchup_retry_at=catchup_cursor.next_retry_at if catchup_cursor else None,
            catchup_rate_limit_streak=catchup_cursor.rate_limit_streak if catchup_cursor else 0,
            last_catchup_attempt_at=catchup_cursor.last_attempt_at if catchup_cursor else None,
            last_catchup_error=catchup_cursor.last_error if catchup_cursor else None,
            operational_recency=OperationalRecencyView.model_validate(operational_recency),
        ).model_dump(mode="json")
        with self.session_factory() as session:
            payload["alarmHarvest"] = self._build_alarm_harvest_overview(session, company_slug=company_slug)
        return payload

    def _combine_operational_recency(self, recencies: list[dict[str, Any]]) -> dict[str, Any]:
        normalized = [OperationalRecencyView.model_validate(item) for item in recencies if item]
        if not normalized:
            return OperationalRecencyView().model_dump(mode="json")

        def _latest_datetime(values: list[datetime | None]) -> datetime | None:
            sanitized = [value for value in values if value is not None]
            return max(sanitized) if sanitized else None

        latest_pending_source = max(
            normalized,
            key=lambda item: ensure_utc(item.last_pending_review_at) or ensure_utc(item.last_pending_visibility_at) or datetime.min.replace(tzinfo=ZoneInfo("UTC")),
        )

        return OperationalRecencyView(
            last_raw_dms_at=_latest_datetime([ensure_utc(item.last_raw_dms_at) for item in normalized]),
            last_accepted_dms_at=_latest_datetime([ensure_utc(item.last_accepted_dms_at) for item in normalized]),
            last_visible_dms_at=_latest_datetime([ensure_utc(item.last_visible_dms_at) for item in normalized]),
            last_pending_review_at=_latest_datetime([ensure_utc(item.last_pending_review_at) for item in normalized]),
            last_pending_visibility_at=_latest_datetime([ensure_utc(item.last_pending_visibility_at) for item in normalized]),
            pending_review_count=sum(item.pending_review_count for item in normalized),
            pending_actionable_count=sum(item.pending_actionable_count for item in normalized),
            pending_visibility_count=sum(item.pending_visibility_count for item in normalized),
            latest_pending_reason=latest_pending_source.latest_pending_reason,
            latest_pending_plate=latest_pending_source.latest_pending_plate,
        ).model_dump(mode="json")

    def _build_operational_recency(
        self,
        session: Any,
        *,
        company: CompanyConfig | None,
        reference_at: datetime | None = None,
    ) -> dict[str, Any]:
        if company is None:
            return OperationalRecencyView().model_dump(mode="json")

        now_utc = ensure_utc(reference_at) or utc_now()
        tz = ZoneInfo(company.timezone or self.settings.default_timezone)
        month_start_local = now_utc.astimezone(tz).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        month_start_utc = month_start_local.astimezone(ZoneInfo("UTC"))
        decided_rows = session.execute(
            select(ReconciliationReview.guid, ReconciliationReview.review_status).where(
                ReconciliationReview.company_slug == company.slug,
                ReconciliationReview.guid.is_not(None),
                ReconciliationReview.review_status.in_(("approved", "discarded")),
            )
        ).all()
        discarded_guids = {guid for guid, review_status in decided_rows if guid and review_status == "discarded"}
        visible_statuses = ("visible_episode", "fused_in_episode")

        accepted_stmt = select(func.max(AlarmEvent.occurred_at)).where(
            AlarmEvent.company_slug == company.slug,
            AlarmEvent.source.in_(ACTIVE_EVENT_SOURCES),
            AlarmEvent.classification_status == "classified_dms",
            AlarmEvent.occurred_at >= month_start_utc,
        )
        visible_stmt = select(func.max(AlarmEvent.occurred_at)).where(
            AlarmEvent.company_slug == company.slug,
            AlarmEvent.source.in_(ACTIVE_EVENT_SOURCES),
            AlarmEvent.classification_status == "classified_dms",
            AlarmEvent.visibility_status.in_(visible_statuses),
            AlarmEvent.occurred_at >= month_start_utc,
        )
        if discarded_guids:
            accepted_stmt = accepted_stmt.where(AlarmEvent.guid.not_in(discarded_guids))
            visible_stmt = visible_stmt.where(AlarmEvent.guid.not_in(discarded_guids))

        last_accepted_dms_at = ensure_utc(session.scalar(accepted_stmt))
        last_visible_dms_at = ensure_utc(session.scalar(visible_stmt))
        last_raw_dms_at = ensure_utc(
            session.scalar(
                select(func.max(func.coalesce(HowenAlarmRaw.occurred_at, HowenAlarmRaw.received_at))).where(
                    HowenAlarmRaw.company_slug == company.slug,
                    HowenAlarmRaw.classification_status == "classified_dms",
                    or_(
                        HowenAlarmRaw.occurred_at >= month_start_utc,
                        HowenAlarmRaw.received_at >= month_start_utc,
                    ),
                )
            )
        )

        pending_filters = [
            ReconciliationReview.company_slug == company.slug,
            ReconciliationReview.review_status == "pending",
            or_(
                ReconciliationReview.observed_at >= month_start_utc,
                ReconciliationReview.created_at >= month_start_utc,
            ),
        ]
        pending_counts = dict(
            session.execute(
                select(ReconciliationReview.suggested_action, func.count())
                .where(*pending_filters)
                .group_by(ReconciliationReview.suggested_action)
            ).all()
        )
        latest_pending = session.scalars(
            select(ReconciliationReview)
            .where(*pending_filters)
            .order_by(ReconciliationReview.observed_at.desc(), ReconciliationReview.created_at.desc())
            .limit(1)
        ).first()
        latest_visibility = session.scalars(
            select(ReconciliationReview)
            .where(*pending_filters, ReconciliationReview.suggested_action == "review_visibility")
            .order_by(ReconciliationReview.observed_at.desc(), ReconciliationReview.created_at.desc())
            .limit(1)
        ).first()

        pending_review_count = int(sum(pending_counts.values()))
        pending_visibility_count = int(pending_counts.get("review_visibility", 0) or 0)
        pending_actionable_count = max(pending_review_count - pending_visibility_count, 0)
        latest_pending_at = ensure_utc(latest_pending.observed_at) or ensure_utc(latest_pending.created_at) if latest_pending else None
        latest_visibility_at = (
            ensure_utc(latest_visibility.observed_at) or ensure_utc(latest_visibility.created_at)
            if latest_visibility
            else None
        )

        return OperationalRecencyView(
            last_raw_dms_at=last_raw_dms_at,
            last_accepted_dms_at=last_accepted_dms_at,
            last_visible_dms_at=last_visible_dms_at,
            last_pending_review_at=latest_pending_at,
            last_pending_visibility_at=latest_visibility_at,
            pending_review_count=pending_review_count,
            pending_actionable_count=pending_actionable_count,
            pending_visibility_count=pending_visibility_count,
            latest_pending_reason=latest_pending.reason if latest_pending else None,
            latest_pending_plate=latest_pending.plate_no if latest_pending else None,
        ).model_dump(mode="json")

    def build_admin_overview(self, company_slug: str | None = None) -> dict[str, Any]:
        cache_key = company_slug or "__global__"
        cached = self._admin_overview_cache.get(cache_key)
        now_monotonic = monotonic()
        if cached and now_monotonic - cached[0] < ADMIN_OVERVIEW_CACHE_SECONDS:
            return cached[1]

        company = self.registry.get(company_slug) if company_slug else None
        target_companies = [company] if company else self.registry.all()
        if not target_companies:
            raise RuntimeError("No hay empresas configuradas")
        now_utc = utc_now()
        target_company_slugs = {item.slug for item in target_companies}
        company_today_by_slug = {
            item.slug: now_utc.astimezone(ZoneInfo(item.timezone or self.settings.default_timezone)).date()
            for item in target_companies
        }
        snapshot_dates_today = set(company_today_by_slug.values())
        with self.session_factory() as session:
            publications = {
                row.company_slug: row
                for row in session.scalars(select(PublishedDashboardSnapshot))
                if row.company_slug in target_company_slugs
            }
            latest_rebuilds = self._latest_activation_rebuilds(session)
            ready_companies = [
                item
                for item in target_companies
                if self._company_ready_for_selector(
                    publication=publications.get(item.slug),
                    rebuild=latest_rebuilds.get(item.slug),
                )
            ]
            devices = [
                device
                for device in session.scalars(select(DeviceRecord).where(DeviceRecord.record_source == "live").order_by(DeviceRecord.plate_no))
                if any(self.registry.device_belongs(item, device.device_id, device.fleet_id) for item in ready_companies)
            ]
            reports = list(
                session.scalars(
                    (
                        select(ReportAsset)
                        .where(ReportAsset.company_slug == company_slug)
                        .order_by(ReportAsset.year.desc(), ReportAsset.month.desc())
                        if company_slug
                        else select(ReportAsset).order_by(ReportAsset.year.desc(), ReportAsset.month.desc())
                    )
                )
            )
            snapshots_today = list(
                session.scalars(
                    select(DailyMileageSnapshot).where(
                        DailyMileageSnapshot.source.in_(ACTIVE_SNAPSHOT_SOURCES),
                        DailyMileageSnapshot.snapshot_date.in_(snapshot_dates_today),
                    )
                )
            )
            dms_alarms_24h = list(
                session.scalars(
                    select(AlarmEvent).where(
                        AlarmEvent.source.in_(ACTIVE_EVENT_SOURCES),
                        AlarmEvent.classification_status == "classified_dms",
                        AlarmEvent.occurred_at >= now_utc - timedelta(hours=24),
                    )
                )
            )
            raw_metrics = self._load_raw_alarm_metrics(
                session,
                company=company,
                company_slug=company_slug,
                cutoff=now_utc - timedelta(hours=24),
            )
            if company and company in ready_companies:
                operational_recency = self._build_operational_recency(session, company=company, reference_at=now_utc)
            else:
                operational_recency = self._combine_operational_recency(
                    [self._build_operational_recency(session, company=item, reference_at=now_utc) for item in ready_companies]
                )

        company_snapshots = [
            (item, self.build_snapshot(item.slug))
            for item in ready_companies
        ]
        if not company_snapshots and company:
            publication = publications.get(company.slug)
            if publication and publication.snapshot_json:
                company_snapshots = [(company, self._load_published_snapshot(company.slug) or self.build_snapshot(company.slug))]
        if not company_snapshots:
            primary_company = target_companies[0]
            primary_snapshot = {
                "meta": {
                    "rangeEnd": company_today_by_slug[primary_company.slug].isoformat(),
                    "kmTotal": 0.0,
                    "kmTotalClosedWindow": 0.0,
                    "currentDayKmProvisional": 0.0,
                },
                "reports": [],
                "dataQuality": {"active_notes": [], "anomaly_count_24h": 0},
                "feed": {"status": "sin_datos", "label": "Sin datos"},
            }
            company_snapshots = [(primary_company, primary_snapshot)]
        primary_company, primary_snapshot = company_snapshots[0]
        last_day = date.fromisoformat(primary_snapshot["meta"]["rangeEnd"])

        def resolve_target_company(*, device_id: str | None, fleet_id: str | None) -> CompanyConfig:
            resolved = self.registry.resolve_company(device_id=device_id, fleet_id=fleet_id)
            if resolved and resolved.slug in target_company_slugs:
                return resolved
            return primary_company

        stale_vehicles = sum(
            1
            for device in devices
            if (
                not device.last_received_at
                or (
                    now_utc - ensure_utc(device.last_received_at)
                ).total_seconds()
                / 60
                >= resolve_target_company(device_id=device.device_id, fleet_id=device.fleet_id).rules.feed_stopped_threshold_minutes
            )
        )
        vehicles_reporting_status_24h = sum(
            1
            for device in devices
            if device.last_received_at and ensure_utc(device.last_received_at) >= now_utc - timedelta(hours=24)
        )
        vehicles_with_status_today = sum(
            1
            for device in devices
            if device.last_received_at
            and (
                local_received_at := ensure_utc(device.last_received_at).astimezone(
                    ZoneInfo(
                        resolve_target_company(
                            device_id=device.device_id,
                            fleet_id=device.fleet_id,
                        ).timezone
                        or self.settings.default_timezone
                    )
                )
            ).date()
            == company_today_by_slug[
                resolve_target_company(device_id=device.device_id, fleet_id=device.fleet_id).slug
            ]
        )
        vehicles_with_snapshot_today = len(
            {
                snapshot_row.device_id
                for snapshot_row in snapshots_today
                if (
                    target_company := self.registry.resolve_company(
                        device_id=snapshot_row.device_id,
                        fleet_id=snapshot_row.fleet_id,
                    )
                )
                and target_company.slug in company_today_by_slug
                and snapshot_row.snapshot_date == company_today_by_slug[target_company.slug]
            }
        )
        vehicles_with_valid_day_km_today = sum(1 for device in devices if _is_valid_day_km(device.last_day_km, device.last_total_km))
        vehicles_missing_day_km_today = max(len(devices) - vehicles_with_valid_day_km_today, 0)
        latest_report = reports[0] if reports else None
        active_notes: list[dict[str, Any]] = []
        for snapshot_company, snapshot in company_snapshots:
            for note in snapshot["dataQuality"]["active_notes"]:
                active_notes.append(
                    {
                        **note,
                        "title": note["title"] if len(target_companies) == 1 else f"{snapshot_company.name}: {note['title']}",
                    }
                )

        payload = AdminOverviewView(
            company_slug=company.slug if company else "all",
            company_name=company.name if company else "Todas las empresas",
            ingest_mode="live",
            feed=FeedState.model_validate(primary_snapshot["feed"]),
            coverage=CoverageSummaryView(
                total_vehicles=len(devices),
                vehicles_reporting_status_24h=vehicles_reporting_status_24h,
                vehicles_with_any_alarm_24h=raw_metrics["vehicles_with_any_alarm_24h"],
                vehicles_with_dms_alarm_24h=len(
                    {
                        alarm.device_id
                        for alarm in dms_alarms_24h
                        if any(self.registry.device_belongs(item, alarm.device_id, alarm.fleet_id) for item in target_companies)
                    }
                ),
                vehicles_with_live_dms_24h=raw_metrics["vehicles_with_live_dms_24h"],
                vehicles_with_valid_day_km_today=vehicles_with_valid_day_km_today,
                vehicles_missing_day_km_today=vehicles_missing_day_km_today,
                vehicles_with_status_today=vehicles_with_status_today,
                stale_vehicles=stale_vehicles,
                vehicles_with_snapshot_today=vehicles_with_snapshot_today,
            ),
            km=KmSummaryView(
                total_window_km=round(sum(snapshot["meta"]["kmTotal"] for _, snapshot in company_snapshots), 1),
                closed_window_km=round(sum(snapshot["meta"]["kmTotalClosedWindow"] for _, snapshot in company_snapshots), 1),
                current_day_km_provisional=round(sum(snapshot["meta"]["currentDayKmProvisional"] for _, snapshot in company_snapshots), 1),
                current_day_label=last_day,
            ),
            reports=ReportsSummaryView(
                available_reports=sum(len(snapshot["reports"]) for _, snapshot in company_snapshots),
                latest_report_year=latest_report.year if latest_report else None,
                latest_report_month=latest_report.month if latest_report else None,
            ),
            publication=_build_publication_state(company=primary_company, settings=self.settings),
            anomaly_count_24h=sum(snapshot["dataQuality"]["anomaly_count_24h"] for _, snapshot in company_snapshots),
            active_notes=active_notes,
            operational_recency=OperationalRecencyView.model_validate(operational_recency),
        ).model_dump(mode="json")
        with self.session_factory() as session:
            payload["alarmHarvest"] = self._build_alarm_harvest_overview(session, company_slug=company_slug)
        self._admin_overview_cache[cache_key] = (now_monotonic, payload)
        return payload

    def build_admin_company_catalog(self) -> dict[str, Any]:
        now_monotonic = monotonic()
        if self._admin_company_catalog_cache and now_monotonic - self._admin_company_catalog_cache[0] < ADMIN_COMPANY_CATALOG_CACHE_SECONDS:
            return self._admin_company_catalog_cache[1]

        self.registry.reload()
        companies = sorted(
            self.registry.all(),
            key=lambda company: (not self.registry.is_operational(company), company.name.lower(), company.slug),
        )
        recent_status_cutoff = utc_now() - timedelta(hours=24)
        recent_alarm_cutoff = utc_now() - timedelta(days=7)
        assigned_by_fleet: dict[str, CompanyConfig] = {}
        companies_by_slug = {company.slug: company for company in companies}
        for company in companies:
            for fleet_id in company.fleet_ids:
                assigned_by_fleet[fleet_id] = company

        with self.session_factory() as session:
            devices = list(
                session.scalars(
                    select(DeviceRecord)
                    .where(DeviceRecord.record_source == "live")
                    .order_by(DeviceRecord.fleet_id, DeviceRecord.plate_no, DeviceRecord.device_id)
                )
            )
            recent_alarms = list(
                session.scalars(
                    select(AlarmEvent)
                    .where(
                        AlarmEvent.source.in_(ACTIVE_EVENT_SOURCES),
                        AlarmEvent.occurred_at >= recent_alarm_cutoff,
                    )
                    .order_by(AlarmEvent.occurred_at.desc())
                )
            )
            publications = {
                row.company_slug: row
                for row in session.scalars(select(PublishedDashboardSnapshot))
            }
            latest_rebuilds: dict[str, CompanyHistoricalRebuildJob] = {}
            for rebuild in session.scalars(
                select(CompanyHistoricalRebuildJob)
                .where(CompanyHistoricalRebuildJob.purpose == "activation_bootstrap")
                .order_by(CompanyHistoricalRebuildJob.created_at.desc(), CompanyHistoricalRebuildJob.id.desc())
            ):
                latest_rebuilds.setdefault(rebuild.company_slug, rebuild)

        for device in devices:
            device.last_received_at = ensure_utc(device.last_received_at)
            device.last_seen_at = ensure_utc(device.last_seen_at)
        for alarm in recent_alarms:
            alarm.occurred_at = ensure_utc(alarm.occurred_at) or alarm.occurred_at

        fleet_rows: dict[str, dict[str, Any]] = {}
        for device in devices:
            if _is_mock_identity(device.device_id, device.fleet_id) or not device.fleet_id:
                continue
            assigned_company = assigned_by_fleet.get(device.fleet_id)
            row = fleet_rows.setdefault(
                device.fleet_id,
                {
                    "fleet_id": device.fleet_id,
                    "fleet_name": device.fleet_name,
                    "total_devices": 0,
                    "devices_with_status": 0,
                    "devices_seen_24h": 0,
                    "alarm_events_7d": 0,
                    "latest_seen_at": None,
                    "latest_alarm_at": None,
                    "sample_plates": [],
                    "selected": assigned_company is not None,
                    "assigned_company_slug": assigned_company.slug if assigned_company else None,
                    "assigned_company_name": assigned_company.name if assigned_company else None,
                },
            )
            row["total_devices"] += 1
            if device.last_received_at:
                row["devices_with_status"] += 1
            if device.last_seen_at and device.last_seen_at >= recent_status_cutoff:
                row["devices_seen_24h"] += 1
            if device.last_seen_at and (row["latest_seen_at"] is None or device.last_seen_at > row["latest_seen_at"]):
                row["latest_seen_at"] = device.last_seen_at
            if not row["fleet_name"] and device.fleet_name:
                row["fleet_name"] = device.fleet_name
            if device.plate_no and device.plate_no not in row["sample_plates"] and len(row["sample_plates"]) < 5:
                row["sample_plates"].append(device.plate_no)

        for alarm in recent_alarms:
            if _is_mock_identity(alarm.device_id, alarm.fleet_id) or not alarm.fleet_id:
                continue
            assigned_company = assigned_by_fleet.get(alarm.fleet_id)
            row = fleet_rows.setdefault(
                alarm.fleet_id,
                {
                    "fleet_id": alarm.fleet_id,
                    "fleet_name": None,
                    "total_devices": 0,
                    "devices_with_status": 0,
                    "devices_seen_24h": 0,
                    "alarm_events_7d": 0,
                    "latest_seen_at": None,
                    "latest_alarm_at": None,
                    "sample_plates": [],
                    "selected": assigned_company is not None,
                    "assigned_company_slug": assigned_company.slug if assigned_company else None,
                    "assigned_company_name": assigned_company.name if assigned_company else None,
                },
            )
            row["alarm_events_7d"] += 1
            if row["latest_alarm_at"] is None or alarm.occurred_at > row["latest_alarm_at"]:
                row["latest_alarm_at"] = alarm.occurred_at
            if alarm.plate_no and alarm.plate_no not in row["sample_plates"] and len(row["sample_plates"]) < 5:
                row["sample_plates"].append(alarm.plate_no)

        activation_jobs: list[dict[str, Any]] = []
        for company in companies:
            item = self._serialize_admin_company_catalog_item(
                company=company,
                publication=publications.get(company.slug),
                rebuild=latest_rebuilds.get(company.slug),
            )
            if item["operational"] and not item["ready_in_selector"]:
                activation_jobs.append(item)
        for slug, rebuild in latest_rebuilds.items():
            if slug in companies_by_slug or rebuild.status not in {"queued", "running", "failed"}:
                continue
            activation_jobs.append(
                self._serialize_admin_company_activation_job(
                    slug=slug,
                    name=slug.replace("-", " ").strip() or slug,
                    timezone=self.settings.default_timezone,
                    rebuild=rebuild,
                    can_deactivate=False,
                )
            )

        payload = {
            "total_companies": len(companies),
            "operational_companies": sum(1 for company in companies if self.registry.is_operational(company)),
            "companies": [
                self._serialize_admin_company_catalog_item(
                    company=company,
                    publication=publications.get(company.slug),
                    rebuild=latest_rebuilds.get(company.slug),
                )
                for company in companies
            ],
            "activation_jobs": activation_jobs,
            "fleet_candidates": [
                FleetCandidateView(**row).model_dump(mode="json")
                for row in sorted(
                    fleet_rows.values(),
                    key=lambda item: (
                        item["assigned_company_slug"] is not None,
                        -item["alarm_events_7d"],
                        -item["devices_seen_24h"],
                        -item["total_devices"],
                        item["fleet_name"] or item["fleet_id"],
                    ),
                )
            ],
        }
        self._admin_company_catalog_cache = (now_monotonic, payload)
        return payload

    def _serialize_admin_company_catalog_item(
        self,
        *,
        company: CompanyConfig,
        publication: PublishedDashboardSnapshot | None,
        rebuild: CompanyHistoricalRebuildJob | None,
    ) -> dict[str, Any]:
        rebuild_progress_pct: float | None = None
        rebuild_status = "idle"
        rebuild_days_done = 0
        rebuild_days_total = 0
        rebuild_started_at = None
        rebuild_finished_at = None
        rebuild_next_retry_at = None
        rebuild_published_cut_at = None
        rebuild_error_message = None

        if rebuild:
            rebuild_status = rebuild.status
            rebuild_days_done = rebuild.days_done or 0
            rebuild_days_total = rebuild.days_total or 0
            rebuild_started_at = ensure_utc(rebuild.started_at)
            rebuild_finished_at = ensure_utc(rebuild.finished_at)
            rebuild_next_retry_at = ensure_utc(rebuild.next_retry_at)
            rebuild_published_cut_at = ensure_utc(rebuild.published_cut_at)
            rebuild_error_message = rebuild.error_message
            if rebuild_days_total > 0:
                rebuild_progress_pct = min(100.0, round((rebuild_days_done / rebuild_days_total) * 100.0, 1))

        ready_in_selector = bool(
            self.registry.is_operational(company)
            and publication
            and publication.snapshot_json
            and rebuild_status not in {"queued", "running", "failed"}
        )

        return {
            "slug": company.slug,
            "name": company.name,
            "customer": company.customer,
            "timezone": company.timezone,
            "subdomain": company.subdomain,
            "fleet_ids": list(company.fleet_ids),
            "device_ids": list(company.device_ids),
            "operational": self.registry.is_operational(company),
            "ready_in_selector": ready_in_selector,
            "rebuild_status": "ready" if ready_in_selector and rebuild_status in {"idle", "succeeded"} else rebuild_status,
            "rebuild_progress_pct": rebuild_progress_pct,
            "rebuild_days_done": rebuild_days_done,
            "rebuild_days_total": rebuild_days_total,
            "rebuild_phase": rebuild.phase if rebuild else None,
            "rebuild_rows_total": (rebuild.rows_total or 0) if rebuild else 0,
            "rebuild_rows_processed": (rebuild.rows_processed or 0) if rebuild else 0,
            "rebuild_current_device_id": rebuild.current_device_id if rebuild else None,
            "rebuild_last_heartbeat_at": ensure_utc(rebuild.last_heartbeat_at) if rebuild else None,
            "rebuild_started_at": rebuild_started_at,
            "rebuild_finished_at": rebuild_finished_at,
            "rebuild_next_retry_at": rebuild_next_retry_at,
            "rebuild_published_cut_at": rebuild_published_cut_at,
            "rebuild_error_message": rebuild_error_message,
            "can_deactivate": True,
        }

    def _serialize_admin_company_activation_job(
        self,
        *,
        slug: str,
        name: str,
        timezone: str,
        rebuild: CompanyHistoricalRebuildJob,
        can_deactivate: bool,
    ) -> dict[str, Any]:
        rebuild_days_done = rebuild.days_done or 0
        rebuild_days_total = rebuild.days_total or 0
        rebuild_progress_pct = None
        if rebuild_days_total > 0:
            rebuild_progress_pct = min(100.0, round((rebuild_days_done / rebuild_days_total) * 100.0, 1))
        return {
            "slug": slug,
            "name": name,
            "customer": name,
            "timezone": timezone,
            "subdomain": None,
            "fleet_ids": [],
            "device_ids": [],
            "operational": False,
            "ready_in_selector": False,
            "rebuild_status": rebuild.status,
            "rebuild_progress_pct": rebuild_progress_pct,
            "rebuild_days_done": rebuild_days_done,
            "rebuild_days_total": rebuild_days_total,
            "rebuild_phase": rebuild.phase,
            "rebuild_rows_total": rebuild.rows_total or 0,
            "rebuild_rows_processed": rebuild.rows_processed or 0,
            "rebuild_current_device_id": rebuild.current_device_id,
            "rebuild_last_heartbeat_at": ensure_utc(rebuild.last_heartbeat_at),
            "rebuild_started_at": ensure_utc(rebuild.started_at),
            "rebuild_finished_at": ensure_utc(rebuild.finished_at),
            "rebuild_next_retry_at": ensure_utc(rebuild.next_retry_at),
            "rebuild_published_cut_at": ensure_utc(rebuild.published_cut_at),
            "rebuild_error_message": rebuild.error_message,
            "can_deactivate": can_deactivate,
        }

    def _build_alarm_harvest_overview(self, session: Any, *, company_slug: str | None = None) -> dict[str, Any]:
        current_cut_at = _current_harvest_cut(
            interval_minutes=self.settings.harvest_cut_interval_minutes,
            lag_seconds=self.settings.harvest_window_lag_seconds,
        )
        query = select(AlarmHarvestRun).order_by(AlarmHarvestRun.cut_at.desc(), AlarmHarvestRun.updated_at.desc())
        if company_slug:
            query = query.where(AlarmHarvestRun.company_slug == company_slug)
        runs = list(session.scalars(query))
        latest_by_company: dict[str, AlarmHarvestRun] = {}
        for run in runs:
            latest_by_company.setdefault(run.company_slug, run)
        latest_rebuilds = self._latest_activation_rebuilds(session)
        if company_slug:
            latest_rebuilds = {slug: row for slug, row in latest_rebuilds.items() if slug == company_slug}

        bootstrap_slugs = {
            slug
            for slug, row in latest_rebuilds.items()
            if row.status in {"queued", "running"}
        }
        latest_cut_runs = [
            latest
            for slug, latest in latest_by_company.items()
            if slug not in bootstrap_slugs
        ]
        running_cuts = sum(1 for run in latest_cut_runs if run.status == "running")
        queued_cuts = sum(1 for run in latest_cut_runs if run.status == "queued")
        active_rebuilds = sum(1 for row in latest_rebuilds.values() if row.status == "running")
        queued_rebuilds = sum(1 for row in latest_rebuilds.values() if row.status == "queued")
        queue_depth = running_cuts + queued_cuts + active_rebuilds + queued_rebuilds
        delayed_companies = 0
        rate_limited_companies = 0
        completed_companies = 0
        for slug, latest in latest_by_company.items():
            if slug in bootstrap_slugs:
                continue
            cut_at = ensure_utc(latest.cut_at)
            if latest.status == "rate_limited":
                rate_limited_companies += 1
            if latest.status == "succeeded" and cut_at == current_cut_at:
                completed_companies += 1
            elif latest.status in {"partial", "failed", "rate_limited"} or (cut_at and cut_at < current_cut_at):
                delayed_companies += 1
        return {
            "currentCutAt": current_cut_at.isoformat(),
            "completedCompanies": completed_companies,
            "delayedCompanies": delayed_companies,
            "rateLimitedCompanies": rate_limited_companies,
            "queueDepth": queue_depth,
            "runningCuts": running_cuts,
            "queuedCuts": queued_cuts,
            "activeRebuilds": active_rebuilds,
            "queuedRebuilds": queued_rebuilds,
            "bootstrappingCompanies": len(bootstrap_slugs),
        }

    def _build_admin_recent_metrics(
        self,
        *,
        company: CompanyConfig,
        alarms: list[AlarmEvent],
        raw_rows: list[HowenAlarmRaw],
        baseline_snapshots: list[DailyMileageSnapshot],
        review_status_by_guid: dict[str, str],
        tz: ZoneInfo,
        daily_km_by_vehicle: dict[str, dict[date, float]] | None = None,
        fleet_vehicle_count: int | None = None,
    ) -> dict[str, Any]:
        visible_alarms = [event for event in alarms if event.classification_status == "classified_dms"]
        effective_daily_km_by_vehicle = daily_km_by_vehicle
        if effective_daily_km_by_vehicle is None:
            effective_daily_km_by_vehicle, _ = _build_daily_km(baseline_snapshots, [], alarms, tz)
        analysis = _build_recent_episode_analysis(
            visible_alarms,
            company,
            tz,
            effective_daily_km_by_vehicle,
            review_status_by_guid=review_status_by_guid,
            fleet_vehicle_count=fleet_vehicle_count,
        )
        metrics = dict(analysis["metrics"])
        metrics["raw_events"] = sum(
            1
            for row in raw_rows
            if row.classification_status == "classified_dms" and row.temporal_status == "accepted"
        )
        metrics["non_dms_hidden"] = sum(
            1
            for row in raw_rows
            if row.classification_status == "classified_non_dms" and row.temporal_status == "accepted"
        )
        metrics["unmapped_hidden"] = sum(1 for row in raw_rows if row.classification_status == "unmapped")
        metrics["future_rejected"] = sum(1 for row in raw_rows if row.temporal_status == "future_rejected")
        return metrics

    def build_admin_live_setup(
        self,
        company_slug: str,
        *,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> dict[str, Any]:
        company = self.registry.get(company_slug)
        now_utc = utc_now()
        window_end = ensure_utc(end_at) or now_utc
        recent_alarm_cutoff = ensure_utc(start_at) or (window_end - timedelta(days=7))
        recent_status_cutoff = now_utc - timedelta(hours=24)

        with self.session_factory() as session:
            devices = list(session.scalars(select(DeviceRecord).order_by(DeviceRecord.fleet_id, DeviceRecord.plate_no, DeviceRecord.device_id)))
            recent_alarms = list(
                session.scalars(
                    select(AlarmEvent)
                    .where(AlarmEvent.occurred_at >= recent_alarm_cutoff, AlarmEvent.occurred_at <= window_end)
                    .order_by(AlarmEvent.occurred_at.desc())
                )
            )
            recent_raw_alarms = list(
                session.scalars(
                    select(HowenAlarmRaw)
                    .where(
                        or_(
                            HowenAlarmRaw.received_at.between(recent_alarm_cutoff, window_end),
                            HowenAlarmRaw.occurred_at.between(recent_alarm_cutoff, window_end),
                        )
                    )
                    .order_by(HowenAlarmRaw.received_at.desc())
                )
            )
            all_alarms = list(session.scalars(select(AlarmEvent)))
            all_snapshots = list(session.scalars(select(DailyMileageSnapshot)))

        for device in devices:
            device.last_received_at = ensure_utc(device.last_received_at)
            device.last_seen_at = ensure_utc(device.last_seen_at)
        for alarm in recent_alarms:
            alarm.occurred_at = ensure_utc(alarm.occurred_at) or alarm.occurred_at
        for raw_alarm in recent_raw_alarms:
            raw_alarm.occurred_at = ensure_utc(raw_alarm.occurred_at)
            raw_alarm.received_at = ensure_utc(raw_alarm.received_at) or raw_alarm.received_at
        for alarm in all_alarms:
            alarm.occurred_at = ensure_utc(alarm.occurred_at) or alarm.occurred_at
        for snapshot in all_snapshots:
            snapshot.observed_at = ensure_utc(snapshot.observed_at) or snapshot.observed_at

        company_devices = [device for device in devices if self.registry.device_belongs(company, device.device_id, device.fleet_id)]
        company_snapshots = [snapshot for snapshot in all_snapshots if self.registry.device_belongs(company, snapshot.device_id, snapshot.fleet_id)]
        company_alarms = [alarm for alarm in all_alarms if self.registry.device_belongs(company, alarm.device_id, alarm.fleet_id)]

        assignment = CompanyAssignmentView(
            company_slug=company.slug,
            fleet_ids=list(company.fleet_ids),
            device_ids=list(company.device_ids),
            points_to_mock=any(_is_mock_device_id(device_id) for device_id in company.device_ids)
            or any(_is_mock_fleet_id(fleet_id) for fleet_id in company.fleet_ids),
            visible_devices=len(company_devices),
            visible_mock_devices=sum(1 for device in company_devices if _is_mock_identity(device.device_id, device.fleet_id)),
            visible_real_devices=sum(1 for device in company_devices if not _is_mock_identity(device.device_id, device.fleet_id)),
            visible_snapshots=len(company_snapshots),
            visible_mock_snapshots=sum(1 for snapshot in company_snapshots if _is_mock_identity(snapshot.device_id, snapshot.fleet_id)),
            visible_real_snapshots=sum(1 for snapshot in company_snapshots if not _is_mock_identity(snapshot.device_id, snapshot.fleet_id)),
            visible_alarms=len(company_alarms),
            visible_mock_alarms=sum(1 for alarm in company_alarms if _is_mock_identity(alarm.device_id, alarm.fleet_id)),
            visible_real_alarms=sum(1 for alarm in company_alarms if not _is_mock_identity(alarm.device_id, alarm.fleet_id)),
        )

        mock_data = MockDataSummaryView(
            devices_total=len(devices),
            devices_mock=sum(1 for device in devices if _is_mock_identity(device.device_id, device.fleet_id)),
            devices_real=sum(1 for device in devices if not _is_mock_identity(device.device_id, device.fleet_id)),
            snapshots_total=len(all_snapshots),
            snapshots_mock=sum(1 for snapshot in all_snapshots if _is_mock_identity(snapshot.device_id, snapshot.fleet_id)),
            snapshots_real=sum(1 for snapshot in all_snapshots if not _is_mock_identity(snapshot.device_id, snapshot.fleet_id)),
            alarms_total=len(all_alarms),
            alarms_mock=sum(1 for alarm in all_alarms if _is_mock_identity(alarm.device_id, alarm.fleet_id)),
            alarms_real=sum(1 for alarm in all_alarms if not _is_mock_identity(alarm.device_id, alarm.fleet_id)),
        )

        fleet_rows: dict[str, dict[str, Any]] = {}
        for device in devices:
            if _is_mock_identity(device.device_id, device.fleet_id) or not device.fleet_id:
                continue
            row = fleet_rows.setdefault(
                device.fleet_id,
                {
                    "fleet_id": device.fleet_id,
                    "fleet_name": device.fleet_name,
                    "total_devices": 0,
                    "devices_with_status": 0,
                    "devices_seen_24h": 0,
                    "alarm_events_7d": 0,
                    "latest_seen_at": None,
                    "latest_alarm_at": None,
                    "sample_plates": [],
                    "selected": device.fleet_id in set(company.fleet_ids),
                },
            )
            row["total_devices"] += 1
            if device.last_received_at:
                row["devices_with_status"] += 1
            if device.last_seen_at and device.last_seen_at >= recent_status_cutoff:
                row["devices_seen_24h"] += 1
            if device.last_seen_at and (row["latest_seen_at"] is None or device.last_seen_at > row["latest_seen_at"]):
                row["latest_seen_at"] = device.last_seen_at
            if not row["fleet_name"] and device.fleet_name:
                row["fleet_name"] = device.fleet_name
            if device.plate_no and device.plate_no not in row["sample_plates"] and len(row["sample_plates"]) < 5:
                row["sample_plates"].append(device.plate_no)

        for alarm in recent_alarms:
            if _is_mock_identity(alarm.device_id, alarm.fleet_id) or not alarm.fleet_id:
                continue
            row = fleet_rows.setdefault(
                alarm.fleet_id,
                {
                    "fleet_id": alarm.fleet_id,
                    "fleet_name": None,
                    "total_devices": 0,
                    "devices_with_status": 0,
                    "devices_seen_24h": 0,
                    "alarm_events_7d": 0,
                    "latest_seen_at": None,
                    "latest_alarm_at": None,
                    "sample_plates": [],
                    "selected": alarm.fleet_id in set(company.fleet_ids),
                },
            )
            row["alarm_events_7d"] += 1
            if row["latest_alarm_at"] is None or alarm.occurred_at > row["latest_alarm_at"]:
                row["latest_alarm_at"] = alarm.occurred_at
            if alarm.plate_no and alarm.plate_no not in row["sample_plates"] and len(row["sample_plates"]) < 5:
                row["sample_plates"].append(alarm.plate_no)

        fleet_candidates = [
            FleetCandidateView(**row).model_dump(mode="json")
            for row in sorted(
                fleet_rows.values(),
                key=lambda item: (
                    not item["selected"],
                    -item["alarm_events_7d"],
                    -item["devices_seen_24h"],
                    -item["total_devices"],
                    item["fleet_id"],
                ),
            )
        ]

        unclassified_groups: dict[tuple[str | None, str | None], dict[str, Any]] = {}
        for alarm in recent_raw_alarms:
            if _is_mock_identity(alarm.device_id or "", alarm.fleet_id) or alarm.classification_status != "unmapped":
                continue
            key = (alarm.raw_tp, alarm.raw_event_code)
            row = unclassified_groups.setdefault(
                key,
                {
                    "subtype": alarm.raw_tp,
                    "event_code": alarm.raw_event_code,
                    "count": 0,
                    "sample_device_id": alarm.device_id,
                    "sample_plate": alarm.plate_no,
                },
            )
            row["count"] += 1

        unclassified_codes = [
            UnclassifiedCodeView(**row).model_dump(mode="json")
            for row in sorted(unclassified_groups.values(), key=lambda item: (-item["count"], item["subtype"] or "", item["event_code"] or ""))[:12]
        ]

        recent_raw_diagnostics = [
            _serialize_raw_alarm_diagnostic(alarm)
            for alarm in recent_raw_alarms
            if not _is_mock_identity(alarm.device_id or "", alarm.fleet_id)
            and self.registry.device_belongs(company, alarm.device_id, alarm.fleet_id)
            and (
                alarm.classification_status != "classified_dms"
                or alarm.temporal_status != "accepted"
                or alarm.ingest_result not in {"inserted_alarm_event", "updated_alarm_event"}
            )
        ][:18]

        return AdminLiveSetupView(
            company_slug=company.slug,
            company_name=company.name,
            assignment=CompanyAssignmentView.model_validate(assignment),
            mock_data=MockDataSummaryView.model_validate(mock_data),
            fleet_candidates=[FleetCandidateView.model_validate(item) for item in fleet_candidates],
            unclassified_codes=[UnclassifiedCodeView.model_validate(item) for item in unclassified_codes],
            recent_raw_diagnostics=[RawAlarmDiagnosticView.model_validate(item) for item in recent_raw_diagnostics],
        ).model_dump(mode="json")

    def purge_mock_legacy(self) -> dict[str, Any]:
        with self.session_factory() as session:
            mock_device_filter = or_(
                DeviceRecord.device_id.like(f"{MOCK_DEVICE_PREFIX}%"),
                DeviceRecord.fleet_id.in_(MOCK_FLEET_IDS),
            )
            mock_snapshot_filter = or_(
                DailyMileageSnapshot.device_id.like(f"{MOCK_DEVICE_PREFIX}%"),
                DailyMileageSnapshot.fleet_id.in_(MOCK_FLEET_IDS),
            )
            mock_alarm_filter = or_(
                AlarmEvent.device_id.like(f"{MOCK_DEVICE_PREFIX}%"),
                AlarmEvent.fleet_id.in_(MOCK_FLEET_IDS),
            )
            mock_mileage_filter = or_(
                MileageReading.device_id.like(f"{MOCK_DEVICE_PREFIX}%"),
                MileageReading.fleet_id.in_(MOCK_FLEET_IDS),
            )

            deleted_devices = session.query(DeviceRecord).filter(mock_device_filter).count()
            deleted_snapshots = session.query(DailyMileageSnapshot).filter(mock_snapshot_filter).count()
            deleted_alarms = session.query(AlarmEvent).filter(mock_alarm_filter).count()
            deleted_mileage_readings = session.query(MileageReading).filter(mock_mileage_filter).count()

            session.query(DailyMileageSnapshot).filter(mock_snapshot_filter).delete(synchronize_session=False)
            session.query(AlarmEvent).filter(mock_alarm_filter).delete(synchronize_session=False)
            session.query(MileageReading).filter(mock_mileage_filter).delete(synchronize_session=False)
            session.query(DeviceRecord).filter(mock_device_filter).delete(synchronize_session=False)
            session.commit()

        return MockDataPurgeResult(
            deleted_devices=deleted_devices,
            deleted_snapshots=deleted_snapshots,
            deleted_alarms=deleted_alarms,
            deleted_mileage_readings=deleted_mileage_readings,
        ).model_dump(mode="json")

    def build_admin_audit(self, company_slug: str, start_at: datetime, end_at: datetime) -> dict[str, Any]:
        cache_key = f"{company_slug}:{ensure_utc(start_at).isoformat()}:{ensure_utc(end_at).isoformat()}"
        cached = self._admin_audit_cache.get(cache_key)
        now_monotonic = monotonic()
        if cached and now_monotonic - cached[0] < ADMIN_AUDIT_CACHE_SECONDS:
            return cached[1]
        company = self.registry.get(company_slug)
        tz = ZoneInfo(company.timezone or self.settings.default_timezone)
        baseline_start = ensure_utc(end_at).astimezone(tz).date() - timedelta(days=30)
        recent_7d_start_at = ensure_utc(end_at) - timedelta(days=7)
        recent_start_at = ensure_utc(end_at) - timedelta(hours=24)
        query_start_at = min(ensure_utc(start_at), recent_7d_start_at, recent_start_at)

        def _membership_clause(model: Any) -> Any:
            company_field = getattr(model, "company_slug", None)
            if company_field is not None:
                return company_field == company_slug
            return None

        with self.session_factory() as session:
            review_status_by_guid = _load_review_status_map(session, company_slug)
            fleet_vehicle_count = session.scalar(
                select(func.count(DeviceRecord.device_id)).where(DeviceRecord.company_slug == company_slug)
            ) or 0
            alarm_membership = _membership_clause(AlarmEvent)
            raw_membership = _membership_clause(HowenAlarmRaw)
            snapshot_membership = _membership_clause(DailyMileageSnapshot)

            alarm_query = (
                select(AlarmEvent)
                .where(
                    AlarmEvent.source.in_(ACTIVE_EVENT_SOURCES),
                    AlarmEvent.occurred_at >= query_start_at,
                    AlarmEvent.occurred_at <= end_at,
                )
                .order_by(AlarmEvent.occurred_at)
            )
            if alarm_membership is not None:
                alarm_query = alarm_query.where(alarm_membership)
            window_alarm_rows = [
                event for event in session.scalars(alarm_query) if review_status_by_guid.get(event.guid) != "discarded"
            ]

            raw_query = (
                select(HowenAlarmRaw)
                .where(
                    or_(
                        HowenAlarmRaw.occurred_at.between(query_start_at, end_at),
                        HowenAlarmRaw.received_at.between(query_start_at, end_at),
                    )
                )
                .order_by(HowenAlarmRaw.received_at.desc())
            )
            if raw_membership is not None:
                raw_query = raw_query.where(raw_membership)
            window_raw_rows = list(session.scalars(raw_query))
            anomalies = list(
                session.scalars(
                    select(IngestionAnomaly)
                    .where(
                        IngestionAnomaly.company_slug == company_slug,
                        IngestionAnomaly.received_at >= start_at,
                        IngestionAnomaly.received_at <= end_at,
                    )
                    .order_by(IngestionAnomaly.received_at.desc())
                )
            )
            alarm_audits = list(
                session.scalars(
                    select(AlarmEventAudit)
                    .where(
                        AlarmEventAudit.company_slug == company_slug,
                        AlarmEventAudit.received_at >= start_at,
                        AlarmEventAudit.received_at <= end_at,
                    )
                    .order_by(AlarmEventAudit.received_at.desc())
                )
            )
            baseline_snapshots = [
                snapshot
                for snapshot in session.scalars(
                    (
                        select(DailyMileageSnapshot)
                        .where(
                            DailyMileageSnapshot.source.in_(ACTIVE_SNAPSHOT_SOURCES),
                            DailyMileageSnapshot.snapshot_date >= baseline_start,
                        )
                        .where(snapshot_membership if snapshot_membership is not None else True)
                        .order_by(DailyMileageSnapshot.snapshot_date, DailyMileageSnapshot.observed_at)
                    )
                )
            ]

        for event in window_alarm_rows:
            event.occurred_at = ensure_utc(event.occurred_at) or event.occurred_at
        for raw_alarm in window_raw_rows:
            raw_alarm.occurred_at = ensure_utc(raw_alarm.occurred_at)
            raw_alarm.received_at = ensure_utc(raw_alarm.received_at) or raw_alarm.received_at
        for snapshot in baseline_snapshots:
            snapshot.observed_at = ensure_utc(snapshot.observed_at) or snapshot.observed_at
        for audit_row in alarm_audits:
            audit_row.received_at = ensure_utc(audit_row.received_at) or audit_row.received_at
            audit_row.observed_at = ensure_utc(audit_row.observed_at)

        def _alarm_in_window(event: AlarmEvent, window_start: datetime) -> bool:
            return bool(event.occurred_at and event.occurred_at >= window_start and event.occurred_at <= end_at)

        def _raw_in_window(row: HowenAlarmRaw, window_start: datetime) -> bool:
            occurred_in_range = bool(row.occurred_at and row.occurred_at >= window_start and row.occurred_at <= end_at)
            received_in_range = bool(row.received_at and row.received_at >= window_start and row.received_at <= end_at)
            return occurred_in_range or received_in_range

        all_company_alarms = [event for event in window_alarm_rows if _alarm_in_window(event, start_at)]
        recent_company_alarms = [event for event in window_alarm_rows if _alarm_in_window(event, recent_start_at)]
        recent_7d_company_alarms = [event for event in window_alarm_rows if _alarm_in_window(event, recent_7d_start_at)]
        raw_company_alarms = [row for row in window_raw_rows if _raw_in_window(row, start_at)]
        recent_raw_company_alarms = [row for row in window_raw_rows if _raw_in_window(row, recent_start_at)]
        recent_7d_raw_company_alarms = [row for row in window_raw_rows if _raw_in_window(row, recent_7d_start_at)]
        daily_km_by_vehicle, _ = _build_daily_km(baseline_snapshots, [], window_alarm_rows, tz)

        requested_metrics = self._build_admin_recent_metrics(
            company=company,
            alarms=all_company_alarms,
            raw_rows=raw_company_alarms,
            baseline_snapshots=baseline_snapshots,
            review_status_by_guid=review_status_by_guid,
            tz=tz,
            daily_km_by_vehicle=daily_km_by_vehicle,
            fleet_vehicle_count=fleet_vehicle_count,
        )
        recent_metrics = self._build_admin_recent_metrics(
            company=company,
            alarms=recent_company_alarms,
            raw_rows=recent_raw_company_alarms,
            baseline_snapshots=baseline_snapshots,
            review_status_by_guid=review_status_by_guid,
            tz=tz,
            daily_km_by_vehicle=daily_km_by_vehicle,
            fleet_vehicle_count=fleet_vehicle_count,
        )
        recent_7d_metrics = self._build_admin_recent_metrics(
            company=company,
            alarms=recent_7d_company_alarms,
            raw_rows=recent_7d_raw_company_alarms,
            baseline_snapshots=baseline_snapshots,
            review_status_by_guid=review_status_by_guid,
            tz=tz,
            daily_km_by_vehicle=daily_km_by_vehicle,
            fleet_vehicle_count=fleet_vehicle_count,
        )

        payload = AdminAuditView(
            company_slug=company.slug,
            company_name=company.name,
            range_start=start_at,
            range_end=end_at,
            alarms=AlarmAuditView(
                accepted_total=len(all_company_alarms),
                visible_total=sum(1 for event in all_company_alarms if event.classification_status == "classified_dms"),
                unclassified_total=sum(1 for event in all_company_alarms if event.classification_status == "unmapped"),
                mapping_sources=dict(Counter(event.mapping_source or "unknown" for event in all_company_alarms)),
                by_category=dict(Counter(event.category for event in all_company_alarms)),
                audit_stages=dict(Counter(audit_row.stage for audit_row in alarm_audits)),
                audit_reasons=dict(Counter(audit_row.reason for audit_row in alarm_audits)),
                by_subtype=[
                    {"subtype": subtype or "sin_subtipo", "count": count}
                    for subtype, count in Counter(
                        event.raw_alarm_type or event.subtype or event.raw_tp or "" for event in all_company_alarms
                    ).most_common(20)
                ],
            ),
            anomalies=AnomalyAuditView(
                total=len(anomalies),
                by_reason=dict(Counter(anomaly.reason for anomaly in anomalies)),
            ),
            requested_window=RecentAuditView(**requested_metrics),
            recent_7d=RecentAuditView(**recent_7d_metrics),
            recent_24h=RecentAuditView(**recent_metrics),
        ).model_dump(mode="json")
        self._admin_audit_cache[cache_key] = (now_monotonic, payload)
        return payload

    async def run_reconciliation(
        self,
        payload: ReconciliationRunRequest,
        *,
        start_task: bool = True,
    ) -> dict[str, Any]:
        company = self.registry.get(payload.company_slug)
        range_start, range_end = _resolve_reconciliation_range(
            company=company,
            start_at=payload.from_at,
            end_at=payload.to_at,
            window_type=payload.window_type,
            fallback_timezone=self.settings.default_timezone,
        )
        params_hash = self._reconciliation_params_hash(
            company_slug=company.slug,
            range_start=range_start,
            range_end=range_end,
            window_type=payload.window_type,
        )
        now = utc_now()
        job_id: str | None = None
        with self.session_factory() as session:
            existing_job = session.scalar(
                select(ReconciliationJob)
                .where(
                    ReconciliationJob.company_slug == company.slug,
                    ReconciliationJob.params_hash == params_hash,
                )
                .order_by(ReconciliationJob.updated_at.desc())
            )
            if existing_job and existing_job.status == "succeeded" and existing_job.result_expires_at and ensure_utc(existing_job.result_expires_at) > now:
                return self.get_reconciliation_job(existing_job.id)
            if existing_job and existing_job.status in {"queued", "running"}:
                job_id = existing_job.id
                self._ensure_reconciliation_device_rows(session, existing_job, company)
                session.commit()
            elif existing_job and existing_job.status in {"rate_limited", "failed"}:
                job_id = existing_job.id
                self._prepare_reconciliation_job_for_resume(session, existing_job, company)
            else:
                job = ReconciliationJob(
                    id=uuid4().hex,
                    company_slug=company.slug,
                    params_hash=params_hash,
                    window_type=payload.window_type,
                    range_start=range_start,
                    range_end=range_end,
                    status="queued",
                )
                session.add(job)
                session.flush()
                job_id = job.id
                self._ensure_reconciliation_device_rows(session, job, company)
                session.commit()
        if not job_id:
            raise RuntimeError("No se pudo crear o reutilizar el job de conciliacion")
        if start_task:
            self._ensure_reconciliation_task(job_id)
        job_payload = self.get_reconciliation_job(job_id)
        return ReconciliationRunResponse(
            job_id=job_payload["job_id"],
            status=job_payload["status"],
            cached_result_available=job_payload.get("cached_result_available", False),
            total_devices=job_payload.get("total_devices", 0),
            processed_devices=job_payload.get("processed_devices", 0),
            succeeded_devices=job_payload.get("succeeded_devices", 0),
            failed_devices=job_payload.get("failed_devices", 0),
            rate_limited_devices=job_payload.get("rate_limited_devices", 0),
            current_device_id=job_payload.get("current_device_id"),
            range_start=job_payload["range_start"],
            range_end=job_payload["range_end"],
            window_type=job_payload["window_type"],
            summary=ReconciliationSummary.model_validate(job_payload["summary"]) if job_payload.get("summary") else None,
            drilldown=[ReconciliationDrilldownRow.model_validate(item) for item in job_payload.get("drilldown", [])],
        ).model_dump(mode="json")

    async def build_reconciliation_summary(
        self,
        *,
        company_slug: str,
        start_at: datetime,
        end_at: datetime,
        window_type: str,
    ) -> dict[str, Any]:
        latest = self.get_latest_reconciliation(
            company_slug=company_slug,
            start_at=start_at,
            end_at=end_at,
            window_type=window_type,
        )
        return latest["summary"] if latest.get("summary") else {}

    async def build_reconciliation_drilldown(
        self,
        *,
        company_slug: str,
        start_at: datetime,
        end_at: datetime,
        window_type: str,
    ) -> list[dict[str, Any]]:
        latest = self.get_latest_reconciliation(
            company_slug=company_slug,
            start_at=start_at,
            end_at=end_at,
            window_type=window_type,
        )
        return latest.get("drilldown", [])

    def list_reconciliation_reviews(
        self,
        *,
        company_slug: str,
        start_at: datetime,
        end_at: datetime,
        review_status: str = "pending",
        limit: int = 60,
        sync_queue: bool = False,
        suggested_actions: list[str] | None = None,
    ) -> dict[str, Any]:
        company = self.registry.get(company_slug)
        if sync_queue and self._should_sync_operational_review_queue(company=company, start_at=start_at, end_at=end_at):
            self._sync_operational_review_queue(
                company=company,
                start_at=start_at,
                end_at=end_at,
            )
        with self.session_factory() as session:
            filters = [
                ReconciliationReview.company_slug == company_slug,
                ReconciliationReview.review_status == review_status,
                or_(
                    ReconciliationReview.observed_at.between(start_at, end_at),
                    ReconciliationReview.created_at.between(start_at, end_at),
                ),
            ]
            if suggested_actions:
                filters.append(ReconciliationReview.suggested_action.in_(suggested_actions))
            base_query = select(ReconciliationReview).where(*filters)
            total_items = session.scalar(
                select(func.count()).select_from(base_query.order_by(None).subquery())
            ) or 0
            counts_by_action = {
                action or "sin_accion": count
                for action, count in session.execute(
                    select(ReconciliationReview.suggested_action, func.count())
                    .where(*filters)
                    .group_by(ReconciliationReview.suggested_action)
                ).all()
            }
            counts_by_reason = {
                reason or "sin_motivo": count
                for reason, count in session.execute(
                    select(ReconciliationReview.reason, func.count())
                    .where(*filters)
                    .group_by(ReconciliationReview.reason)
                ).all()
            }
            rows = list(
                session.scalars(
                    base_query
                    .order_by(ReconciliationReview.observed_at.desc(), ReconciliationReview.created_at.desc())
                    .limit(max(1, min(limit, 200)))
                )
            )
        return ReconciliationReviewListView(
            total_items=total_items,
            counts_by_action=counts_by_action,
            counts_by_reason=counts_by_reason,
            items=[ReconciliationReviewItemView.model_validate(_serialize_reconciliation_review(row)) for row in rows],
        ).model_dump(mode="json")

    def _should_sync_operational_review_queue(
        self,
        *,
        company: CompanyConfig,
        start_at: datetime,
        end_at: datetime,
    ) -> bool:
        with self.session_factory() as session:
            latest_review_update = session.scalar(
                select(func.max(ReconciliationReview.updated_at)).where(
                    ReconciliationReview.company_slug == company.slug,
                    or_(
                        ReconciliationReview.observed_at.between(start_at, end_at),
                        ReconciliationReview.created_at.between(start_at, end_at),
                    ),
                )
            )
            latest_source_points = [
                session.scalar(
                    select(func.max(HowenAlarmRaw.received_at)).where(
                        HowenAlarmRaw.company_slug == company.slug,
                        HowenAlarmRaw.classification_status == "classified_dms",
                        or_(
                            HowenAlarmRaw.occurred_at.between(start_at, end_at),
                            HowenAlarmRaw.received_at.between(start_at, end_at),
                        ),
                    )
                ),
                session.scalar(
                    select(func.max(AlarmEvent.occurred_at)).where(
                        AlarmEvent.company_slug == company.slug,
                        AlarmEvent.classification_status == "classified_dms",
                        AlarmEvent.occurred_at.between(start_at, end_at),
                    )
                ),
                session.scalar(
                    select(func.max(IngestionAnomaly.received_at)).where(
                        IngestionAnomaly.company_slug == company.slug,
                        IngestionAnomaly.received_at.between(start_at, end_at),
                    )
                ),
            ]

        latest_source_update = max(
            [ensure_utc(point) for point in latest_source_points if ensure_utc(point)],
            default=None,
        )
        if latest_source_update is None:
            return latest_review_update is None
        latest_review_update = ensure_utc(latest_review_update)
        return latest_review_update is None or latest_source_update > latest_review_update

    def _sync_operational_review_queue(
        self,
        *,
        company: CompanyConfig,
        start_at: datetime,
        end_at: datetime,
    ) -> None:
        self._sync_anomaly_review_queue(company=company, start_at=start_at, end_at=end_at)
        self._sync_problem_raw_review_queue(company=company, start_at=start_at, end_at=end_at)
        self._sync_km_review_queue(company=company, start_at=start_at, end_at=end_at)
        self._sync_suppressed_review_queue(company=company, start_at=start_at, end_at=end_at)

    def _sync_anomaly_review_queue(
        self,
        *,
        company: CompanyConfig,
        start_at: datetime,
        end_at: datetime,
    ) -> None:
        with self.session_factory() as session:
            anomalies = list(
                session.scalars(
                    select(IngestionAnomaly)
                    .where(
                        IngestionAnomaly.company_slug == company.slug,
                        IngestionAnomaly.received_at.between(start_at, end_at),
                    )
                    .order_by(IngestionAnomaly.received_at.desc())
                )
            )
            for anomaly in anomalies:
                payload = _parse_json(anomaly.payload_json)
                if not payload:
                    continue
                normalized = self.howen.normalize_alarm(payload)
                if not normalized:
                    continue
                if normalized.classification_status != "classified_dms":
                    continue
                if not self.registry.device_belongs(company, normalized.device_id, normalized.fleet_id):
                    continue

                existing_alarm = session.get(AlarmEvent, normalized.guid)
                if existing_alarm and existing_alarm.classification_status == "classified_dms":
                    continue

                existing_raw = session.get(HowenAlarmRaw, normalized.guid)
                if (
                    existing_raw
                    and existing_raw.classification_status == "classified_dms"
                    and existing_raw.temporal_status == "accepted"
                    and existing_raw.ingest_result == "inserted_alarm_event"
                ):
                    continue

                review_key = f"anomaly:{anomaly.id}"
                review = session.scalar(
                    select(ReconciliationReview).where(ReconciliationReview.review_key == review_key)
                )
                if not review:
                    review = ReconciliationReview(
                        review_key=review_key,
                        company_slug=company.slug,
                        review_status="pending",
                    )

                review.guid = normalized.guid
                review.device_id = normalized.device_id
                review.plate_no = normalized.plate_no
                review.observed_at = ensure_utc(normalized.occurred_at) or normalized.occurred_at
                review.portal_begin_time = normalized.raw_event_time
                review.portal_reporting_time = normalized.raw_event_time
                review.raw_alarm_type = normalized.raw_alarm_type
                review.raw_tp = normalized.raw_tp
                review.raw_event_code = normalized.raw_event_code
                review.classification_status = normalized.classification_status
                review.visibility_status = "anomaly_pending_review"
                review.category = normalized.category
                review.subtype = normalized.subtype
                review.reason = anomaly.reason
                review.diagnostic_note = (
                    "Esta alerta fue capturada como anomalia de ingesta. Requiere supervision humana para decidir si se incorpora al dashboard operativo o se descarta."
                )
                review.suggested_action = "review_anomaly"
                review.source_job_id = None
                review.source_window_type = "calendar_month_local"
                review.portal_payload_json = anomaly.payload_json
                session.add(review)
            session.commit()

    def _sync_problem_raw_review_queue(
        self,
        *,
        company: CompanyConfig,
        start_at: datetime,
        end_at: datetime,
    ) -> None:
        with self.session_factory() as session:
            raw_rows = list(
                session.scalars(
                    select(HowenAlarmRaw)
                    .where(
                        HowenAlarmRaw.company_slug == company.slug,
                        or_(
                            HowenAlarmRaw.occurred_at.between(start_at, end_at),
                            HowenAlarmRaw.received_at.between(start_at, end_at),
                        ),
                        HowenAlarmRaw.classification_status == "classified_dms",
                    )
                    .order_by(HowenAlarmRaw.received_at.desc())
                )
            )
            for row in raw_rows:
                if (
                    row.temporal_status == "accepted"
                    and row.ingest_result in {"inserted_alarm_event", "updated_alarm_event"}
                ):
                    continue
                review_key = f"raw:{row.guid}"
                review = session.scalar(
                    select(ReconciliationReview).where(ReconciliationReview.review_key == review_key)
                )
                if not review:
                    review = ReconciliationReview(
                        review_key=review_key,
                        company_slug=company.slug,
                        review_status="pending",
                    )
                review.guid = row.guid
                review.device_id = row.device_id
                review.plate_no = row.plate_no
                review.observed_at = ensure_utc(row.occurred_at) or ensure_utc(row.received_at) or row.received_at
                review.portal_begin_time = row.raw_event_time
                review.portal_reporting_time = row.raw_event_time
                review.raw_alarm_type = row.raw_alarm_type
                review.raw_tp = row.raw_tp
                review.raw_event_code = row.raw_event_code
                review.classification_status = row.classification_status
                review.visibility_status = row.temporal_status or row.ingest_result or "raw_pending_review"
                review.category = row.mapped_category
                review.subtype = row.raw_alarm_type or row.raw_tp
                review.reason = row.ingest_result or row.temporal_status or "raw_pending_review"
                review.diagnostic_note = (
                    "Esta alerta DMS llego al almacenamiento raw pero no termino de incorporarse con normalidad al flujo operativo. Requiere decision manual."
                )
                review.suggested_action = "review_raw"
                review.source_job_id = None
                review.source_window_type = "calendar_month_local"
                review.portal_payload_json = row.payload_json
                session.add(review)
            session.commit()

    def _sync_suppressed_review_queue(
        self,
        *,
        company: CompanyConfig,
        start_at: datetime,
        end_at: datetime,
    ) -> None:
        tz = ZoneInfo(company.timezone or self.settings.default_timezone)
        baseline_start = ensure_utc(end_at).astimezone(tz).date() - timedelta(days=30)
        with self.session_factory() as session:
            review_status_by_guid = _load_review_status_map(session, company.slug)
            fleet_vehicle_count = session.scalar(
                select(func.count(DeviceRecord.device_id)).where(DeviceRecord.company_slug == company.slug)
            ) or 0
            events = [
                event
                for event in session.scalars(
                    select(AlarmEvent)
                    .where(
                        AlarmEvent.company_slug == company.slug,
                        AlarmEvent.classification_status == "classified_dms",
                        AlarmEvent.occurred_at >= start_at,
                        AlarmEvent.occurred_at <= end_at,
                    )
                    .order_by(AlarmEvent.occurred_at)
                )
                if review_status_by_guid.get(event.guid) != "discarded"
            ]
            baseline_snapshots = [
                snapshot
                for snapshot in session.scalars(
                    select(DailyMileageSnapshot)
                    .where(
                        DailyMileageSnapshot.source.in_(ACTIVE_SNAPSHOT_SOURCES),
                        DailyMileageSnapshot.snapshot_date >= baseline_start,
                    )
                    .order_by(DailyMileageSnapshot.snapshot_date, DailyMileageSnapshot.observed_at)
                )
                if self.registry.device_belongs(company, snapshot.device_id, snapshot.fleet_id)
            ]
            for event in events:
                event.occurred_at = ensure_utc(event.occurred_at) or event.occurred_at
            for snapshot in baseline_snapshots:
                snapshot.observed_at = ensure_utc(snapshot.observed_at) or snapshot.observed_at
            daily_km_by_vehicle, _ = _build_daily_km(baseline_snapshots, [], events, tz)
            episode_analysis = _build_recent_episode_analysis(
                events,
                company,
                tz,
                daily_km_by_vehicle,
                review_status_by_guid=review_status_by_guid,
                fleet_vehicle_count=fleet_vehicle_count,
            )
            guid_status = episode_analysis["guid_status"]

        self._persist_suppressed_rule_reviews(
            company=company,
            events=events,
            guid_status=guid_status,
        )

    def _persist_suppressed_rule_reviews(
        self,
        *,
        company: CompanyConfig,
        events: list[AlarmEvent],
        guid_status: dict[str, dict[str, Any]],
    ) -> None:
        suppressed = [
            (event, guid_status[event.guid])
            for event in events
            if guid_status.get(event.guid, {}).get("visibility_status") == "suppressed_by_rule"
        ]
        review_keys = [f"suppressed:{event.guid}" for event, _status in suppressed]
        analyzed_guids = {event.guid for event in events}
        with self.session_factory() as session:
            existing = {
                review.review_key: review
                for review in (
                    session.scalars(
                        select(ReconciliationReview).where(ReconciliationReview.review_key.in_(review_keys))
                    )
                    if review_keys
                    else []
                )
            }
            pending_rule_reviews = list(
                session.scalars(
                    select(ReconciliationReview).where(
                        ReconciliationReview.company_slug == company.slug,
                        ReconciliationReview.review_status == "pending",
                        ReconciliationReview.suggested_action == "review_visibility",
                    )
                )
            )
            active_review_keys = set(review_keys)
            for review in pending_rule_reviews:
                if review.guid in analyzed_guids and review.review_key not in active_review_keys:
                    review.review_status = "resolved"
                    review.decision_note = "La regla vigente ya no retiene este evento."
                    review.applied_at = utc_now()
                    session.add(review)
            for event, status in suppressed:
                review_key = f"suppressed:{event.guid}"
                review = existing.get(review_key)
                if not review:
                    review = ReconciliationReview(
                        review_key=review_key,
                        company_slug=company.slug,
                        review_status="pending",
                    )
                elif review.review_status in {"approved", "discarded"}:
                    continue
                review.guid = event.guid
                review.device_id = event.device_id
                review.plate_no = event.plate_no
                review.observed_at = event.occurred_at
                review.portal_begin_time = event.raw_event_time
                review.portal_reporting_time = event.raw_event_time
                review.raw_alarm_type = event.raw_alarm_type
                review.raw_tp = event.raw_tp
                review.raw_event_code = event.raw_event_code
                review.classification_status = event.classification_status
                review.visibility_status = "suppressed_by_rule"
                review.category = event.category
                review.subtype = event.subtype
                review.reason = status.get("reason") or "suppressed_by_rule"
                review.diagnostic_note = (
                    "La alerta existe en la base operativa, pero una regla N2 requiere decision humana antes de publicarla."
                )
                review.suggested_action = "review_visibility"
                review.source_job_id = None
                review.source_window_type = "calendar_month_local"
                review.portal_payload_json = json.dumps(
                    {
                        "provider_event_key": event.provider_event_key,
                        "guid": event.guid,
                        "device_id": event.device_id,
                        "category": event.category,
                        "subtype": event.subtype,
                    },
                    ensure_ascii=True,
                )
                session.add(review)
            session.commit()

    def _sync_km_review_queue(
        self,
        *,
        company: CompanyConfig,
        start_at: datetime,
        end_at: datetime,
    ) -> None:
        tz = ZoneInfo(company.timezone or self.settings.default_timezone)
        start_local = ensure_utc(start_at).astimezone(tz).date()
        end_local = ensure_utc(end_at).astimezone(tz).date()

        with self.session_factory() as session:
            devices = [
                device
                for device in session.scalars(select(DeviceRecord).where(DeviceRecord.record_source == "live").order_by(DeviceRecord.plate_no, DeviceRecord.device_id))
                if self.registry.device_belongs(company, device.device_id, device.fleet_id)
            ]
            snapshots = [
                snapshot
                for snapshot in session.scalars(
                    select(DailyMileageSnapshot)
                    .where(
                        DailyMileageSnapshot.snapshot_date >= start_local,
                        DailyMileageSnapshot.snapshot_date <= end_local,
                    )
                    .order_by(DailyMileageSnapshot.snapshot_date.desc(), DailyMileageSnapshot.observed_at.desc())
                )
                if self.registry.device_belongs(company, snapshot.device_id, snapshot.fleet_id)
            ]

            latest_snapshot_by_device: dict[str, DailyMileageSnapshot] = {}
            for snapshot in snapshots:
                latest_snapshot_by_device.setdefault(snapshot.device_id, snapshot)

            for device in devices:
                snapshot = latest_snapshot_by_device.get(device.device_id)
                review_reason: str | None = None
                review_note: str | None = None

                if device.km_validation_reason and "total_regression" in device.km_validation_reason:
                    review_reason = "total_regression"
                    review_note = (
                        "El kilometraje total del vehiculo retrocedio frente a un valor previo. Administracion debe revisarlo y decidir si se acepta como reinicio del odometro o se descarta del seguimiento operativo."
                    )
                else:
                    device_valid = _is_valid_day_km(device.last_day_km, device.last_total_km)
                    snapshot_valid = bool(snapshot and _is_valid_day_km(snapshot.day_km, snapshot.total_km))
                    if not (device_valid or snapshot_valid):
                        raw_reason = (
                            device.km_validation_reason
                            or (snapshot.km_validation_reason if snapshot else None)
                            or "missing_day_km"
                        )
                        review_reason = raw_reason
                        if raw_reason == "day_gt_total":
                            review_note = (
                                "El kilometraje del dia quedo por encima del kilometraje total. Requiere validacion humana antes de volver a usarlo en el tablero operativo."
                            )
                        elif raw_reason == "missing_day_km":
                            review_note = (
                                "El vehiculo no tiene un kilometraje diario confiable para este mes. Puedes aprobarlo como observado o descartarlo para que no siga apareciendo pendiente."
                            )
                        else:
                            review_note = (
                                "El kilometraje de este vehiculo quedo en una condicion invalida para el tablero operativo. Requiere supervision humana."
                            )

                if not review_reason or not review_note:
                    continue

                observed_at = (
                    ensure_utc(device.last_received_at)
                    or (ensure_utc(snapshot.observed_at) if snapshot else None)
                    or ensure_utc(device.last_seen_at)
                    or utc_now()
                )
                issue_date = (
                    snapshot.snapshot_date
                    if snapshot and snapshot.snapshot_date >= start_local and snapshot.snapshot_date <= end_local
                    else observed_at.astimezone(tz).date()
                )
                review_key = f"km:{device.device_id}:{issue_date.isoformat()}:{review_reason}"
                review = session.scalar(
                    select(ReconciliationReview).where(ReconciliationReview.review_key == review_key)
                )
                if not review:
                    review = ReconciliationReview(
                        review_key=review_key,
                        company_slug=company.slug,
                        review_status="pending",
                    )

                review.guid = None
                review.device_id = device.device_id
                review.plate_no = device.plate_no
                review.observed_at = observed_at
                review.portal_begin_time = issue_date.isoformat()
                review.portal_reporting_time = observed_at.isoformat()
                review.raw_alarm_type = "Kilometraje"
                review.raw_tp = None
                review.raw_event_code = None
                review.classification_status = "km_review"
                review.visibility_status = "km_pending_review"
                review.category = "Kilometraje"
                review.subtype = review_reason
                review.reason = review_reason
                review.diagnostic_note = review_note
                review.suggested_action = "review_km"
                review.source_job_id = None
                review.source_window_type = "calendar_month_local"
                review.portal_payload_json = json.dumps(
                    {
                        "type": "km_review",
                        "company_slug": company.slug,
                        "device_id": device.device_id,
                        "plate_no": device.plate_no,
                        "reason": review_reason,
                        "issue_date": issue_date.isoformat(),
                    },
                    ensure_ascii=True,
                )
                session.add(review)
            session.commit()

    def decide_reconciliation_review(
        self,
        *,
        review_id: int,
        action: str,
        decided_by: str,
        note: str | None = None,
    ) -> dict[str, Any]:
        if action not in {"approve", "discard"}:
            raise ValueError("Accion de revision invalida")

        now = utc_now()
        review_payload: dict[str, Any] | None = None
        company_slug: str | None = None
        review_reason: str | None = None
        with self.session_factory() as session:
            review = session.get(ReconciliationReview, review_id)
            if not review:
                return {}
            review.review_status = "approved" if action == "approve" else "discarded"
            review.decision_note = note
            review.decided_by = decided_by
            review.decided_at = now
            session.add(review)
            review_payload = _parse_json(review.portal_payload_json)
            company_slug = review.company_slug
            review_reason = review.reason
            session.commit()

        if action == "approve" and review_payload and company_slug:
            if review_payload.get("type") == "km_review":
                with self.session_factory() as session:
                    review = session.get(ReconciliationReview, review_id)
                    if review:
                        review.applied_at = utc_now()
                        session.add(review)
                        session.commit()
            else:
                company = self.registry.get(company_slug)
                self._sync_portal_rows(
                    company=company,
                    portal_rows=[review_payload],
                    force_temporal_accept=review_reason == "rejected_temporal",
                )
                with self.session_factory() as session:
                    review = session.get(ReconciliationReview, review_id)
                    if review:
                        review.applied_at = utc_now()
                        session.add(review)
                        session.commit()

        with self.session_factory() as session:
            review = session.get(ReconciliationReview, review_id)
            return _serialize_reconciliation_review(review) if review else {}

    def decide_reconciliation_reviews_bulk(
        self,
        *,
        review_ids: list[int],
        action: str,
        decided_by: str,
        note: str | None = None,
    ) -> dict[str, Any]:
        unique_review_ids = [review_id for review_id in dict.fromkeys(review_ids) if review_id > 0]
        decided_items: list[dict[str, Any]] = []
        for review_id in unique_review_ids:
            result = self.decide_reconciliation_review(
                review_id=review_id,
                action=action,
                decided_by=decided_by,
                note=note,
            )
            if result:
                decided_items.append(result)
        return ReconciliationReviewBulkDecisionResponse(
            updated=len(decided_items),
            items=[ReconciliationReviewItemView.model_validate(item) for item in decided_items],
        ).model_dump(mode="json")

    def _reconciliation_devices_for_company(
        self,
        session: Any,
        company: CompanyConfig,
    ) -> list[tuple[str, str | None]]:
        devices: list[tuple[str, str | None]] = []
        seen: set[str] = set()
        for device in session.scalars(select(DeviceRecord).where(DeviceRecord.record_source == "live").order_by(DeviceRecord.device_id)):
            if not self.registry.device_belongs(company, device.device_id, device.fleet_id):
                continue
            if device.device_id in seen:
                continue
            seen.add(device.device_id)
            devices.append((device.device_id, device.plate_no))
        if not devices and company.device_ids:
            for device_id in company.device_ids:
                if device_id in seen:
                    continue
                seen.add(device_id)
                devices.append((device_id, None))
        return devices

    def _ensure_reconciliation_device_rows(
        self,
        session: Any,
        job: ReconciliationJob,
        company: CompanyConfig,
    ) -> int:
        existing_rows = {
            row.device_id: row
            for row in session.scalars(select(ReconciliationJobDevice).where(ReconciliationJobDevice.job_id == job.id))
        }
        devices = self._reconciliation_devices_for_company(session, company)
        for device_id, plate_no in devices:
            if device_id in existing_rows:
                row = existing_rows[device_id]
                if plate_no and not row.plate_no:
                    row.plate_no = plate_no
                    session.add(row)
                continue
            session.add(
                ReconciliationJobDevice(
                    job_id=job.id,
                    device_id=device_id,
                    plate_no=plate_no,
                    status="queued",
                )
            )
        return len(devices)

    def _prepare_reconciliation_job_for_resume(
        self,
        session: Any,
        job: ReconciliationJob,
        company: CompanyConfig,
    ) -> None:
        self._ensure_reconciliation_device_rows(session, job, company)
        rows = list(session.scalars(select(ReconciliationJobDevice).where(ReconciliationJobDevice.job_id == job.id)))
        resumable = False
        for row in rows:
            if row.status in {"running", "failed", "rate_limited"}:
                row.status = "queued"
                row.error_message = None
                row.started_at = None
                row.finished_at = None
                session.add(row)
                resumable = True
        if job.status in {"running", "failed", "rate_limited"} or resumable:
            job.status = "queued"
            job.error_message = None
            job.finished_at = None
            session.add(job)
        session.commit()

    def _reconciliation_progress(
        self,
        session: Any,
        job_id: str,
    ) -> dict[str, Any]:
        rows = list(session.scalars(select(ReconciliationJobDevice).where(ReconciliationJobDevice.job_id == job_id).order_by(ReconciliationJobDevice.id)))
        counts = Counter(row.status for row in rows)
        running_row = next((row for row in rows if row.status == "running"), None)
        return {
            "total_devices": len(rows),
            "processed_devices": counts.get("succeeded", 0) + counts.get("failed", 0) + counts.get("rate_limited", 0),
            "succeeded_devices": counts.get("succeeded", 0),
            "failed_devices": counts.get("failed", 0),
            "rate_limited_devices": counts.get("rate_limited", 0),
            "current_device_id": running_row.device_id if running_row else None,
        }

    def _load_reconciliation_portal_rows(
        self,
        session: Any,
        job_id: str,
    ) -> list[dict[str, Any]]:
        portal_rows: list[dict[str, Any]] = []
        for row in session.scalars(
            select(ReconciliationJobDevice)
            .where(ReconciliationJobDevice.job_id == job_id, ReconciliationJobDevice.status == "succeeded")
            .order_by(ReconciliationJobDevice.id)
        ):
            raw_rows = row.portal_rows_json or "[]"
            try:
                parsed = json.loads(raw_rows)
            except json.JSONDecodeError:
                parsed = []
            if isinstance(parsed, list):
                portal_rows.extend(item for item in parsed if isinstance(item, dict))
        return portal_rows

    def get_reconciliation_job(self, job_id: str) -> dict[str, Any]:
        with self.session_factory() as session:
            job = session.get(ReconciliationJob, job_id)
            if not job:
                return {}
            progress = self._reconciliation_progress(session, job.id)
            summary, drilldown = self._resolve_reconciliation_cached_payload(session, job)
            payload = {
                "job_id": job.id,
                "status": job.status,
                "company_slug": job.company_slug,
                "range_start": ensure_utc(job.range_start) or job.range_start,
                "range_end": ensure_utc(job.range_end) or job.range_end,
                "window_type": job.window_type,
                "cached_result_available": bool(summary),
                "started_at": ensure_utc(job.started_at),
                "finished_at": ensure_utc(job.finished_at),
                "error_message": job.error_message,
                "summary": summary,
                "drilldown": drilldown,
                **progress,
            }
        if payload["status"] in {"queued", "running"}:
            self._ensure_reconciliation_task(payload["job_id"])
        return ReconciliationJobView(
            job_id=payload["job_id"],
            status=payload["status"],
            company_slug=payload["company_slug"],
            range_start=payload["range_start"],
            range_end=payload["range_end"],
            window_type=payload["window_type"],
            cached_result_available=payload["cached_result_available"],
            total_devices=payload["total_devices"],
            processed_devices=payload["processed_devices"],
            succeeded_devices=payload["succeeded_devices"],
            failed_devices=payload["failed_devices"],
            rate_limited_devices=payload["rate_limited_devices"],
            current_device_id=payload["current_device_id"],
            started_at=payload["started_at"],
            finished_at=payload["finished_at"],
            error_message=payload["error_message"],
            summary=ReconciliationSummary.model_validate(payload["summary"]) if payload["summary"] else None,
            drilldown=[ReconciliationDrilldownRow.model_validate(item) for item in payload["drilldown"]],
        ).model_dump(mode="json")

    def get_latest_reconciliation(
        self,
        *,
        company_slug: str,
        start_at: datetime,
        end_at: datetime,
        window_type: str,
    ) -> dict[str, Any]:
        company = self.registry.get(company_slug)
        range_start, range_end = _resolve_reconciliation_range(
            company=company,
            start_at=start_at,
            end_at=end_at,
            window_type=window_type,
            fallback_timezone=self.settings.default_timezone,
        )
        params_hash = self._reconciliation_params_hash(
            company_slug=company_slug,
            range_start=range_start,
            range_end=range_end,
            window_type=window_type,
        )
        with self.session_factory() as session:
            job = session.scalar(
                select(ReconciliationJob)
                .where(
                    ReconciliationJob.company_slug == company_slug,
                    ReconciliationJob.params_hash == params_hash,
                )
                .order_by(ReconciliationJob.updated_at.desc())
            )
            job_id = job.id if job else None
        return self.get_reconciliation_job(job_id) if job_id else {}

    def _ensure_reconciliation_task(self, job_id: str) -> None:
        task = self._reconciliation_tasks.get(job_id)
        if task and not task.done():
            return
        self._reconciliation_tasks[job_id] = asyncio.create_task(
            self._run_reconciliation_job(job_id),
            name=f"reconciliation-{job_id}",
        )

    async def process_reconciliation_job(self, job_id: str) -> dict[str, Any]:
        await self._run_reconciliation_job(job_id)
        return self.get_reconciliation_job(job_id)

    def _resolve_reconciliation_cached_payload(
        self,
        session: Session,
        job: ReconciliationJob,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        summary = _parse_json(job.summary_json) if job.summary_json else None
        drilldown = _parse_json(job.drilldown_json) if job.drilldown_json else []
        if summary:
            return summary, drilldown

        cached_job = session.scalar(
            select(ReconciliationJob)
            .where(
                ReconciliationJob.company_slug == job.company_slug,
                ReconciliationJob.params_hash == job.params_hash,
                ReconciliationJob.summary_json.is_not(None),
            )
            .order_by(ReconciliationJob.updated_at.desc())
        )
        if not cached_job or cached_job.id == job.id:
            return None, []

        cached_summary = _parse_json(cached_job.summary_json) if cached_job.summary_json else None
        cached_drilldown = _parse_json(cached_job.drilldown_json) if cached_job.drilldown_json else []
        return cached_summary, cached_drilldown

    async def _run_reconciliation_job(self, job_id: str) -> None:
        company: CompanyConfig | None = None
        with self.session_factory() as session:
            job = session.get(ReconciliationJob, job_id)
            if not job:
                return
            company_slug = job.company_slug
            company = self.registry.get(company_slug)
            self._ensure_reconciliation_device_rows(session, job, company)
            range_start = ensure_utc(job.range_start) or job.range_start
            range_end = ensure_utc(job.range_end) or job.range_end
            window_type = job.window_type
            job.status = "running"
            job.started_at = job.started_at or utc_now()
            job.finished_at = None
            job.error_message = None
            for device_row in session.scalars(
                select(ReconciliationJobDevice).where(
                    ReconciliationJobDevice.job_id == job_id,
                    ReconciliationJobDevice.status == "running",
                )
            ):
                device_row.status = "queued"
                device_row.error_message = None
                device_row.started_at = None
                device_row.finished_at = None
                session.add(device_row)
            session.add(job)
            session.commit()

        if company is None:
            company = self.registry.get(company_slug)
        lock = self._reconciliation_locks.setdefault(company.slug, asyncio.Lock())
        async with lock:
            fatal_error: str | None = None
            rate_limited_error: str | None = None
            try:
                with self.session_factory() as session:
                    job_rows = list(
                        session.scalars(
                            select(ReconciliationJobDevice)
                            .where(ReconciliationJobDevice.job_id == job_id)
                            .order_by(ReconciliationJobDevice.id)
                        )
                    )
                for index, job_row in enumerate(job_rows):
                    if job_row.status == "succeeded":
                        continue
                    if index:
                        await asyncio.sleep(PORTAL_FETCH_DELAY_SECONDS)
                    with self.session_factory() as session:
                        fresh_row = session.get(ReconciliationJobDevice, job_row.id)
                        fresh_job = session.get(ReconciliationJob, job_id)
                        if not fresh_row or not fresh_job:
                            return
                        fresh_row.status = "running"
                        fresh_row.started_at = fresh_row.started_at or utc_now()
                        fresh_row.finished_at = None
                        fresh_row.error_message = None
                        fresh_job.status = "running"
                        fresh_job.error_message = None
                        fresh_job.finished_at = None
                        session.add(fresh_row)
                        session.add(fresh_job)
                        session.commit()

                    rate_retry_count = 0
                    while True:
                        try:
                            portal_rows = await asyncio.wait_for(
                                self.howen.fetch_historical_alarms_authorized(
                                    device_id=job_row.device_id,
                                    start_at=range_start.astimezone(ZoneInfo(company.timezone or self.settings.default_timezone)),
                                    end_at=range_end.astimezone(ZoneInfo(company.timezone or self.settings.default_timezone)),
                                    force_login=False,
                                ),
                                timeout=RECONCILIATION_DEVICE_TIMEOUT_SECONDS,
                            )
                            with self.session_factory() as session:
                                fresh_row = session.get(ReconciliationJobDevice, job_row.id)
                                if not fresh_row:
                                    return
                                fresh_row.status = "succeeded"
                                fresh_row.row_count = len(portal_rows)
                                fresh_row.portal_rows_json = json.dumps(portal_rows, ensure_ascii=True)
                                fresh_row.error_message = None
                                fresh_row.finished_at = utc_now()
                                session.add(fresh_row)
                                session.commit()
                            break
                        except Exception as exc:
                            if self.howen.is_rate_limited(exc):
                                if rate_retry_count < len(PORTAL_RATE_LIMIT_BACKOFF_SECONDS):
                                    await asyncio.sleep(PORTAL_RATE_LIMIT_BACKOFF_SECONDS[rate_retry_count])
                                    rate_retry_count += 1
                                    continue
                                rate_limited_error = (
                                    f"Howen limito la conciliacion exacta para {company.name} en {job_row.device_id}. "
                                    "Reintenta en unos minutos; el progreso ya quedo guardado."
                                )
                                with self.session_factory() as session:
                                    fresh_row = session.get(ReconciliationJobDevice, job_row.id)
                                    fresh_job = session.get(ReconciliationJob, job_id)
                                    if not fresh_row or not fresh_job:
                                        return
                                    fresh_row.status = "rate_limited"
                                    fresh_row.error_message = rate_limited_error
                                    fresh_row.finished_at = utc_now()
                                    fresh_job.status = "rate_limited"
                                    fresh_job.error_message = rate_limited_error
                                    fresh_job.finished_at = utc_now()
                                    session.add(fresh_row)
                                    session.add(fresh_job)
                                    session.commit()
                                break
                            if isinstance(exc, TimeoutError):
                                fatal_error = (
                                    f"La conciliacion exacta supero {int(RECONCILIATION_DEVICE_TIMEOUT_SECONDS)} s en {job_row.device_id}. "
                                    "Puedes reintentar y continuara desde el ultimo dispositivo completado."
                                )
                            else:
                                fatal_error = str(exc)
                            with self.session_factory() as session:
                                fresh_row = session.get(ReconciliationJobDevice, job_row.id)
                                fresh_job = session.get(ReconciliationJob, job_id)
                                if not fresh_row or not fresh_job:
                                    return
                                fresh_row.status = "failed"
                                fresh_row.error_message = fatal_error
                                fresh_row.finished_at = utc_now()
                                fresh_job.status = "failed"
                                fresh_job.error_message = fatal_error
                                fresh_job.finished_at = utc_now()
                                session.add(fresh_row)
                                session.add(fresh_job)
                                session.commit()
                            break
                    if rate_limited_error or fatal_error:
                        break

                if rate_limited_error or fatal_error:
                    return

                with self.session_factory() as session:
                    progress = self._reconciliation_progress(session, job_id)
                    if progress["succeeded_devices"] != progress["total_devices"]:
                        return
                    portal_rows = self._load_reconciliation_portal_rows(session, job_id)
                summary, rows = self._build_reconciliation_report(
                    company=company,
                    range_start=range_start,
                    range_end=range_end,
                    window_type=window_type,
                    portal_rows=portal_rows,
                )
                self._sync_reconciliation_review_queue(
                    company=company,
                    job_id=job_id,
                    window_type=window_type,
                    drilldown_rows=rows,
                    portal_rows=portal_rows,
                )
                with self.session_factory() as session:
                    fresh_job = session.get(ReconciliationJob, job_id)
                    if not fresh_job:
                        return
                    now = utc_now()
                    fresh_job.status = "succeeded"
                    fresh_job.summary_json = json.dumps(summary, ensure_ascii=True)
                    fresh_job.drilldown_json = json.dumps(rows, ensure_ascii=True)
                    fresh_job.finished_at = now
                    fresh_job.result_expires_at = now + timedelta(minutes=self.settings.reconciliation_cache_ttl_minutes)
                    session.add(fresh_job)
                    session.commit()
            except Exception as exc:  # pragma: no cover - defensive background guard
                with self.session_factory() as session:
                    fresh_job = session.get(ReconciliationJob, job_id)
                    if not fresh_job:
                        return
                    fresh_job.status = "failed"
                    fresh_job.error_message = str(exc)
                    fresh_job.finished_at = utc_now()
                    session.add(fresh_job)
                    session.commit()

    def _reconciliation_params_hash(
        self,
        *,
        company_slug: str,
        range_start: datetime,
        range_end: datetime,
        window_type: str,
    ) -> str:
        payload = f"{company_slug}|{window_type}|{ensure_utc(range_start).isoformat()}|{ensure_utc(range_end).isoformat()}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def build_km_quality(self, company_slug: str) -> dict[str, Any]:
        cached = self._km_quality_cache.get(company_slug)
        now_monotonic = monotonic()
        if cached and now_monotonic - cached[0] < KM_QUALITY_CACHE_SECONDS:
            return cached[1]
        company = self.registry.get(company_slug)
        now_local = utc_now().astimezone(ZoneInfo(company.timezone or self.settings.default_timezone))
        today = now_local.date()
        with self.session_factory() as session:
            devices = [
                device
                for device in session.scalars(select(DeviceRecord).where(DeviceRecord.record_source == "live").order_by(DeviceRecord.plate_no))
                if self.registry.device_belongs(company, device.device_id, device.fleet_id)
            ]
            snapshots = [
                snapshot
                for snapshot in session.scalars(
                    select(DailyMileageSnapshot)
                    .where(DailyMileageSnapshot.snapshot_date == today)
                    .order_by(DailyMileageSnapshot.observed_at.desc())
                )
                if self.registry.device_belongs(company, snapshot.device_id, snapshot.fleet_id)
            ]

        latest_snapshot_by_device: dict[str, DailyMileageSnapshot] = {}
        for snapshot in snapshots:
            latest_snapshot_by_device.setdefault(snapshot.device_id, snapshot)

        valid_day = 0
        invalid_day = 0
        total_regression = 0
        invalid_samples: list[str] = []
        regression_samples: list[str] = []
        missing_day_samples: list[str] = []
        for device in devices:
            snapshot = latest_snapshot_by_device.get(device.device_id)
            if device.km_validation_reason and "total_regression" in device.km_validation_reason:
                total_regression += 1
                if device.plate_no and len(regression_samples) < 8:
                    regression_samples.append(device.plate_no)
            total_reference = device.last_total_km
            device_valid = _is_valid_day_km(device.last_day_km, total_reference)
            snapshot_valid = bool(snapshot and _is_valid_day_km(snapshot.day_km, snapshot.total_km))
            is_valid = device_valid or snapshot_valid
            if is_valid:
                valid_day += 1
            else:
                invalid_day += 1
                if device.plate_no and len(invalid_samples) < 8:
                    invalid_samples.append(device.plate_no)
                if (device.last_day_km is None and (not snapshot or snapshot.day_km is None)) and device.plate_no and len(missing_day_samples) < 8:
                    missing_day_samples.append(device.plate_no)
            if snapshot and snapshot.km_validation_reason and "total_regression" in snapshot.km_validation_reason:
                total_regression += 1
                if device.plate_no and len(regression_samples) < 8 and device.plate_no not in regression_samples:
                    regression_samples.append(device.plate_no)

        repaired_rows = sum(1 for snapshot in snapshots if snapshot.repaired_at is not None)
        payload = KmQualitySummary(
            company_slug=company.slug,
            company_name=company.name,
            total_vehicles=len(devices),
            vehicles_with_valid_day_km=valid_day,
            vehicles_with_invalid_day_km=invalid_day,
            vehicles_with_total_regression=total_regression,
            vehicles_with_snapshot_today=len(latest_snapshot_by_device),
            vehicles_with_status_today=sum(1 for device in devices if ensure_utc(device.last_received_at) and ensure_utc(device.last_received_at).astimezone(ZoneInfo(company.timezone or self.settings.default_timezone)).date() == today),
            current_day_km_source="device_state_validated",
            repaired_rows=repaired_rows,
            sample_invalid_vehicles=invalid_samples,
            sample_total_regression_vehicles=regression_samples,
            sample_missing_day_km_vehicles=missing_day_samples,
        ).model_dump(mode="json")
        self._km_quality_cache[company_slug] = (now_monotonic, payload)
        return payload

    def repair_km(self, payload: KmRepairRequest) -> dict[str, Any]:
        company = self.registry.get(payload.company_slug)
        tz = ZoneInfo(company.timezone or self.settings.default_timezone)
        start_date = payload.start_date or (utc_now().astimezone(tz).date() - timedelta(days=30))
        end_date = payload.end_date or utc_now().astimezone(tz).date()
        start_bound = datetime.combine(start_date, datetime.min.time(), tzinfo=tz).astimezone(ZoneInfo("UTC"))
        end_bound = datetime.combine(end_date + timedelta(days=1), datetime.min.time(), tzinfo=tz).astimezone(ZoneInfo("UTC"))
        repaired_rows = 0

        with self.session_factory() as session:
            devices = [
                device
                for device in session.scalars(select(DeviceRecord).where(DeviceRecord.record_source == "live").order_by(DeviceRecord.device_id))
                if self.registry.device_belongs(company, device.device_id, device.fleet_id)
            ]
            alarms = [
                alarm
                for alarm in session.scalars(
                    select(AlarmEvent)
                    .where(
                        AlarmEvent.occurred_at >= start_bound,
                        AlarmEvent.occurred_at < end_bound,
                    )
                    .order_by(AlarmEvent.occurred_at)
                )
                if self.registry.device_belongs(company, alarm.device_id, alarm.fleet_id)
            ]
            snapshots = [
                snapshot
                for snapshot in session.scalars(
                    select(DailyMileageSnapshot)
                    .where(
                        DailyMileageSnapshot.snapshot_date >= start_date,
                        DailyMileageSnapshot.snapshot_date <= end_date,
                    )
                    .order_by(DailyMileageSnapshot.device_id, DailyMileageSnapshot.snapshot_date, DailyMileageSnapshot.observed_at)
                )
                if self.registry.device_belongs(company, snapshot.device_id, snapshot.fleet_id)
            ]

            alarm_totals_by_device_day: dict[str, dict[date, list[tuple[datetime, float]]]] = defaultdict(lambda: defaultdict(list))
            for alarm in alarms:
                raw_payload = _parse_json(alarm.raw_payload)
                normalized_total = _normalize_distance_payload_value(
                    _nested_value(raw_payload, "totalMileage")
                    or _nested_value(raw_payload, "total")
                )
                if normalized_total is not None and alarm.total_mileage_km != normalized_total:
                    alarm.total_mileage_km = normalized_total
                if alarm.total_mileage_km is None:
                    continue
                normalized_persisted = _normalize_persisted_alarm_km(alarm.total_mileage_km)
                if normalized_persisted is None:
                    continue
                occurred_at = ensure_utc(alarm.occurred_at) or alarm.occurred_at
                day_key = occurred_at.astimezone(tz).date()
                alarm_totals_by_device_day[alarm.device_id][day_key].append((occurred_at, normalized_persisted))

            derived_day_km_by_device_day: dict[str, dict[date, float]] = defaultdict(dict)
            latest_total_by_device: dict[str, float] = {}
            for device_id, days in alarm_totals_by_device_day.items():
                previous_day_max: float | None = None
                for day_key in sorted(days):
                    samples = sorted(days[day_key], key=lambda item: item[0])
                    values = [value for _, value in samples]
                    if not values:
                        continue
                    day_min = min(values)
                    day_max = max(values)
                    derived_km = max(day_max - day_min, 0.0)
                    if derived_km <= 0 and previous_day_max is not None and day_max >= previous_day_max:
                        derived_km = max(day_max - previous_day_max, 0.0)
                    derived_day_km_by_device_day[device_id][day_key] = round(derived_km, 1)
                    previous_day_max = day_max
                    latest_total_by_device[device_id] = day_max

            for device in devices:
                raw_payload = _parse_json(device.raw_payload)
                mileage = raw_payload.get("mileage") if isinstance(raw_payload.get("mileage"), dict) else {}
                normalized_total = _normalize_distance_payload_value(mileage.get("total"))
                normalized_day = _normalize_distance_payload_value(mileage.get("todayDay"))
                derived_total = latest_total_by_device.get(device.device_id)
                derived_day = derived_day_km_by_device_day.get(device.device_id, {}).get(end_date)
                if derived_total is not None and (normalized_total is None or derived_total > normalized_total):
                    normalized_total = derived_total
                if derived_day is not None:
                    normalized_day = derived_day
                if normalized_total is not None:
                    device.last_total_km = normalized_total
                if _is_valid_day_km(normalized_day, normalized_total):
                    device.last_day_km = normalized_day
                    device.km_validation_status = "valid"
                    device.km_validation_reason = None
                else:
                    device.km_validation_status = "invalid"
                    device.km_validation_reason = "day_gt_total" if normalized_day is not None else "missing_day_km"
                device.raw_total_value = _string_or_none(mileage.get("total"))
                device.raw_day_value = _string_or_none(mileage.get("todayDay"))

            snapshots_by_device: dict[str, list[DailyMileageSnapshot]] = defaultdict(list)
            for snapshot in snapshots:
                snapshots_by_device[snapshot.device_id].append(snapshot)

            devices_by_id = {device.device_id: device for device in devices}
            for device_id, rows in snapshots_by_device.items():
                rows.sort(key=lambda item: (item.snapshot_date, item.observed_at))
                previous_total: float | None = None
                for snapshot in rows:
                    derived_total = latest_total_by_device.get(device_id) if snapshot.snapshot_date == end_date else None
                    normalized_total = derived_total if derived_total is not None else snapshot.total_km
                    candidate_day = derived_day_km_by_device_day.get(device_id, {}).get(snapshot.snapshot_date, snapshot.day_km)
                    repaired_day = _sanitize_day_km_value(candidate_day, normalized_total, previous_total)
                    next_validation_reason = None if repaired_day is not None else "day_gt_total"
                    device = devices_by_id.get(device_id)
                    if snapshot.snapshot_date == end_date and device and _is_valid_day_km(device.last_day_km, device.last_total_km):
                        repaired_day = round(device.last_day_km or 0.0, 1)
                        next_validation_reason = None
                        normalized_total = device.last_total_km if device.last_total_km is not None else normalized_total
                    if normalized_total is not None and snapshot.total_km != normalized_total:
                        snapshot.total_km = normalized_total
                        snapshot.repair_reason = "normalized_from_raw_payload"
                        snapshot.repaired_at = utc_now()
                        repaired_rows += 1
                    if repaired_day != snapshot.day_km:
                        snapshot.day_km = repaired_day
                        snapshot.repair_reason = "normalized_from_raw_payload"
                        snapshot.repaired_at = utc_now()
                        repaired_rows += 1
                    snapshot.km_validation_status = "valid" if repaired_day is not None else "invalid"
                    snapshot.km_validation_reason = next_validation_reason
                    previous_total = normalized_total if normalized_total is not None else previous_total

            session.commit()

        return self.build_km_quality(company.slug) | {"repaired_rows": repaired_rows}

    async def _fetch_portal_rows(
        self,
        company: CompanyConfig,
        *,
        range_start: datetime,
        range_end: datetime,
    ) -> list[dict[str, Any]]:
        tz = ZoneInfo(company.timezone or self.settings.default_timezone)
        start_local = ensure_utc(range_start).astimezone(tz)
        end_local = ensure_utc(range_end).astimezone(tz)
        with self.session_factory() as db:
            device_ids = [
                device.device_id
                for device in db.scalars(select(DeviceRecord).where(DeviceRecord.record_source == "live").order_by(DeviceRecord.device_id))
                if self.registry.device_belongs(company, device.device_id, device.fleet_id)
            ]
        if not device_ids and company.device_ids:
            device_ids = list(company.device_ids)

        portal_rows: list[dict[str, Any]] = []
        for index, device_id in enumerate(device_ids):
            auth_retry_used = False
            rate_retry_count = 0
            if index:
                await asyncio.sleep(PORTAL_FETCH_DELAY_SECONDS)
            while True:
                try:
                    portal_rows.extend(
                        await self.howen.fetch_historical_alarms_authorized(
                            device_id=device_id,
                            start_at=start_local,
                            end_at=end_local,
                            force_login=False,
                        )
                    )
                    break
                except Exception as exc:
                    if self.howen.is_auth_error(exc) and not auth_retry_used:
                        auth_retry_used = True
                        await self.howen.invalidate_session()
                        continue
                    if self.howen.is_rate_limited(exc):
                        if rate_retry_count < len(PORTAL_RATE_LIMIT_BACKOFF_SECONDS):
                            await asyncio.sleep(PORTAL_RATE_LIMIT_BACKOFF_SECONDS[rate_retry_count])
                            rate_retry_count += 1
                            continue
                        raise HowenRateLimitError(
                            f"Howen limito la conciliacion exacta para {company.name}. Espera unos minutos y vuelve a intentar."
                        ) from exc
                    raise
        return portal_rows

    def _build_reconciliation_report(
        self,
        *,
        company: CompanyConfig,
        range_start: datetime,
        range_end: datetime,
        window_type: str,
        portal_rows: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        tz = ZoneInfo(company.timezone or self.settings.default_timezone)
        baseline_start = ensure_utc(range_end).astimezone(tz).date() - timedelta(days=30)
        with self.session_factory() as session:
            review_status_by_guid = _load_review_status_map(session, company.slug)
            fleet_vehicle_count = session.scalar(
                select(func.count(DeviceRecord.device_id)).where(DeviceRecord.company_slug == company.slug)
            ) or 0
            local_raw_alarms = [
                row
                for row in session.scalars(
                    select(HowenAlarmRaw)
                    .where(
                        HowenAlarmRaw.received_at >= range_start - timedelta(hours=1),
                        HowenAlarmRaw.received_at <= range_end + timedelta(hours=1),
                    )
                    .order_by(HowenAlarmRaw.received_at)
                )
                if self.registry.device_belongs(company, row.device_id, row.fleet_id)
            ]
            local_alarms = [
                event
                for event in session.scalars(
                    select(AlarmEvent)
                    .where(
                        AlarmEvent.source.in_(ACTIVE_EVENT_SOURCES),
                        AlarmEvent.occurred_at >= range_start,
                        AlarmEvent.occurred_at <= range_end,
                    )
                    .order_by(AlarmEvent.occurred_at)
                )
                if self.registry.device_belongs(company, event.device_id, event.fleet_id)
                and review_status_by_guid.get(event.guid) != "discarded"
            ]
            baseline_snapshots = [
                snapshot
                for snapshot in session.scalars(
                    select(DailyMileageSnapshot)
                    .where(DailyMileageSnapshot.snapshot_date >= baseline_start)
                    .order_by(DailyMileageSnapshot.snapshot_date, DailyMileageSnapshot.observed_at)
                )
                if self.registry.device_belongs(company, snapshot.device_id, snapshot.fleet_id)
            ]

        for raw_alarm in local_raw_alarms:
            raw_alarm.occurred_at = ensure_utc(raw_alarm.occurred_at)
            raw_alarm.received_at = ensure_utc(raw_alarm.received_at) or raw_alarm.received_at
        for event in local_alarms:
            event.occurred_at = ensure_utc(event.occurred_at) or event.occurred_at
        for snapshot in baseline_snapshots:
            snapshot.observed_at = ensure_utc(snapshot.observed_at) or snapshot.observed_at

        local_raw_by_guid = {row.guid: row for row in local_raw_alarms}
        local_by_guid = {event.guid: event for event in local_alarms}
        local_raw_candidates: dict[tuple[str | None, str | None, str | None], list[HowenAlarmRaw]] = defaultdict(list)
        for row in local_raw_alarms:
            normalized_plate = self.registry.normalize_plate(company, row.plate_no)
            local_raw_candidates[
                (
                    row.device_id,
                    normalized_plate,
                    row.raw_alarm_type or row.raw_tp or row.raw_event_code,
                )
            ].append(row)
        dms_local_alarms = [event for event in local_alarms if event.classification_status == "classified_dms"]
        daily_km_by_vehicle, _ = _build_daily_km(baseline_snapshots, [], local_alarms, tz)
        episode_analysis = _build_recent_episode_analysis(
            dms_local_alarms,
            company,
            tz,
            daily_km_by_vehicle,
            review_status_by_guid=review_status_by_guid,
            fleet_vehicle_count=fleet_vehicle_count,
        )
        guid_status = episode_analysis["guid_status"]
        normalized_portal_rows: list[tuple[dict[str, Any], Any, str]] = []
        portal_guid_counts: Counter[str] = Counter()

        for portal_row in portal_rows:
            normalized = self.howen.normalize_alarm(portal_row)
            guid = normalized.guid if normalized else (_payload_guid(portal_row) or f"raw-{len(normalized_portal_rows) + 1}")
            normalized_portal_rows.append((portal_row, normalized, guid))
            portal_guid_counts[guid] += 1

        raw_portal_equivalent = 0
        ingested_live = 0
        ingested_backfill = 0
        classified_dms = 0
        classified_non_dms = 0
        visible_raw_events = 0
        suppressed_by_rule = 0
        rejected_temporal = 0
        unmapped = 0
        missing_local = 0
        rows: list[dict[str, Any]] = []
        seen_guids: set[str] = set()

        for portal_row, normalized, guid in normalized_portal_rows:
            if guid in seen_guids:
                continue
            seen_guids.add(guid)
            raw_portal_equivalent += 1
            portal_begin_time = _payload_begin_time(portal_row)
            portal_reporting_time = _payload_reporting_time(portal_row)
            portal_duplicate_count = portal_guid_counts[guid]
            portal_canonical_plate, portal_alias_applied = self.registry.plate_alias_applied(company, normalized.plate_no if normalized else _payload_plate(portal_row))

            if not normalized:
                unmapped += 1
                rows.append(
                    ReconciliationDrilldownRow(
                        guid=guid,
                        plate_no=_payload_plate(portal_row),
                        device_id=_payload_device(portal_row),
                        observed_hour_local=None,
                        portal_begin_time=portal_begin_time,
                        portal_reporting_time=portal_reporting_time,
                        raw_alarm_type=_payload_alarm_type(portal_row),
                        raw_tp=_payload_alarm_tp(portal_row),
                        raw_event_code=_payload_alarm_event_code(portal_row),
                        observed_at=None,
                        stored_observed_at=None,
                        stored_raw_event_time=None,
                        classification_status="unmapped",
                        visibility_status="hidden_unmapped",
                        source="portal_raw",
                        category=None,
                        subtype=None,
                        reason="normalization_failed",
                        portal_duplicate_count=portal_duplicate_count,
                        diagnostic_note=(
                            f"Proveedor envio {portal_duplicate_count} filas equivalentes para este guid."
                            if portal_duplicate_count > 1
                            else None
                        ),
                    ).model_dump(mode="json")
                )
                continue

            future_rejected = _is_future_event(
                normalized.occurred_at,
                tolerance_minutes=self.settings.anomaly_future_tolerance_minutes,
            )
            local_raw_match = local_raw_by_guid.get(normalized.guid)
            local_match = local_by_guid.get(normalized.guid)
            if local_raw_match and local_raw_match.source == "live":
                ingested_live += 1
            elif local_raw_match and local_raw_match.source in {"backfill", "catchup"}:
                ingested_backfill += 1

            if normalized.classification_status == "classified_dms":
                classified_dms += 1
            elif normalized.classification_status == "classified_non_dms":
                classified_non_dms += 1
            else:
                unmapped += 1

            stored_observed_at = local_match.occurred_at if local_match else (local_raw_match.occurred_at if local_raw_match else None)
            stored_raw_event_time = local_match.raw_event_time if local_match else (local_raw_match.raw_event_time if local_raw_match else None)
            related_raw_match = None
            diagnostic_note_parts: list[str] = []
            reason = "classified_non_dms"
            visibility_status = normalized.visibility_status
            episode_guid = None
            episode_title = None
            source_label = local_raw_match.source if local_raw_match else "portal_raw"
            category = local_match.category if local_match else (local_raw_match.mapped_category if local_raw_match else normalized.category)
            subtype = local_match.subtype if local_match else normalized.subtype

            if future_rejected:
                rejected_temporal += 1
                reason = "rejected_temporal"
                visibility_status = "rejected_temporal"
            elif normalized.classification_status == "classified_non_dms":
                reason = "classified_non_dms"
                visibility_status = "hidden_non_dms"
            elif normalized.classification_status == "unmapped":
                reason = "unmapped"
                visibility_status = "hidden_unmapped"
            elif not local_raw_match:
                missing_local += 1
                reason = "missing_local"
                visibility_status = "missing_local"
                related_raw_match = _find_reporting_time_local_match(
                    local_raw_candidates,
                    company=company,
                    registry=self.registry,
                    company_timezone=company.timezone or self.settings.default_timezone,
                    device_id=normalized.device_id,
                    plate_no=portal_canonical_plate or normalized.plate_no,
                    raw_alarm_type=normalized.raw_alarm_type,
                    raw_tp=normalized.raw_tp,
                    raw_event_code=normalized.raw_event_code,
                    reporting_time=portal_reporting_time,
                )
                if related_raw_match:
                    stored_observed_at = related_raw_match.occurred_at
                    stored_raw_event_time = related_raw_match.raw_event_time
                    diagnostic_note_parts.append(
                        "Coincide por reporting time con una alarma local guardada en otra hora normalizada."
                    )
            elif local_raw_match.classification_status != "classified_dms":
                missing_local += 1
                reason = f"stored_local_{local_raw_match.classification_status or 'unknown'}"
                visibility_status = "missing_dashboard_mapping"
            else:
                local_visibility = guid_status.get(normalized.guid)
                if not local_visibility:
                    suppressed_by_rule += 1
                    reason = "suppressed_by_rule"
                    visibility_status = "suppressed_by_rule"
                else:
                    reason = local_visibility["reason"]
                    visibility_status = local_visibility["visibility_status"]
                    episode_guid = local_visibility.get("episode_guid")
                    episode_title = local_visibility.get("episode_title")
                    if visibility_status in {"visible_episode", "fused_in_episode"}:
                        visible_raw_events += 1
                    elif visibility_status == "suppressed_by_rule":
                        suppressed_by_rule += 1

            if portal_alias_applied and portal_canonical_plate:
                diagnostic_note_parts.append(
                    f"La placa del portal se normalizo al alias canonico {portal_canonical_plate} antes de comparar."
                )

            if portal_duplicate_count > 1:
                diagnostic_note_parts.append(
                    f"Proveedor envio {portal_duplicate_count} filas equivalentes para este guid; localmente se consolida una sola identidad."
                )
            if (
                not related_raw_match
                and stored_observed_at
                and portal_begin_time
                and portal_reporting_time
                and portal_begin_time != portal_reporting_time
            ):
                begin_at = parse_timestamp(portal_begin_time, company.timezone or self.settings.default_timezone)
                report_at = parse_timestamp(portal_reporting_time, company.timezone or self.settings.default_timezone)
                stored_at = ensure_utc(stored_observed_at)
                if report_at and stored_at and abs((stored_at - report_at).total_seconds()) <= 120:
                    diagnostic_note_parts.append(
                        "La hora guardada local coincide mejor con Reporting time que con Begin Time."
                    )
                elif begin_at and stored_at and abs((stored_at - begin_at).total_seconds()) > 120:
                    diagnostic_note_parts.append(
                        "Begin Time y la hora guardada local no coinciden; revisar ajuste temporal del proveedor."
                    )

            rows.append(
                ReconciliationDrilldownRow(
                    guid=normalized.guid,
                    plate_no=(local_match.plate_no if local_match else None) or (local_raw_match.plate_no if local_raw_match else normalized.plate_no),
                    device_id=normalized.device_id,
                    observed_hour_local=normalized.occurred_at.astimezone(tz).strftime("%Y-%m-%d %H:00"),
                    portal_begin_time=portal_begin_time,
                    portal_reporting_time=portal_reporting_time,
                    raw_alarm_type=normalized.raw_alarm_type,
                    raw_tp=normalized.raw_tp,
                    raw_event_code=normalized.raw_event_code,
                    observed_at=normalized.occurred_at,
                    stored_observed_at=stored_observed_at,
                    stored_raw_event_time=stored_raw_event_time,
                    classification_status=normalized.classification_status,
                    visibility_status=visibility_status,
                    source=source_label,
                    category=category,
                    subtype=subtype,
                    reason=reason,
                    episode_guid=episode_guid,
                    episode_title=episode_title,
                    portal_duplicate_count=portal_duplicate_count,
                    diagnostic_note=" ".join(diagnostic_note_parts) or None,
                ).model_dump(mode="json")
            )

        rows.sort(key=lambda item: item["observed_at"] or "", reverse=True)
        return (
            ReconciliationSummary(
                company_slug=company.slug,
                company_name=company.name,
                window_type=window_type,
                range_start=range_start,
                range_end=range_end,
                raw_portal_equivalent=raw_portal_equivalent,
                ingested_live=ingested_live,
                ingested_backfill=ingested_backfill,
                classified_dms=classified_dms,
                classified_non_dms=classified_non_dms,
                visible_episodes=episode_analysis["metrics"]["visible_alerts"],
                visible_raw_events=visible_raw_events,
                suppressed_by_rule=suppressed_by_rule,
                rejected_temporal=rejected_temporal,
                unmapped=unmapped,
                missing_local=missing_local,
            ).model_dump(mode="json"),
            rows,
        )

    def _sync_reconciliation_review_queue(
        self,
        *,
        company: CompanyConfig,
        job_id: str,
        window_type: str,
        drilldown_rows: list[dict[str, Any]],
        portal_rows: list[dict[str, Any]],
    ) -> None:
        payload_by_key: dict[str, dict[str, Any]] = {}
        for portal_row in portal_rows:
            normalized = self.howen.normalize_alarm(portal_row)
            review_key = _build_reconciliation_review_key(
                company=company,
                guid=normalized.guid if normalized else _payload_guid(portal_row),
                device_id=(normalized.device_id if normalized else _payload_device(portal_row)),
                plate_no=(normalized.plate_no if normalized else _payload_plate(portal_row)),
                raw_alarm_type=(normalized.raw_alarm_type if normalized else _payload_alarm_type(portal_row)),
                raw_tp=(normalized.raw_tp if normalized else _payload_alarm_tp(portal_row)),
                raw_event_code=(normalized.raw_event_code if normalized else _payload_alarm_event_code(portal_row)),
                portal_begin_time=_payload_begin_time(portal_row),
                portal_reporting_time=_payload_reporting_time(portal_row),
            )
            payload_by_key[review_key] = portal_row

        with self.session_factory() as session:
            for row in drilldown_rows:
                if not _is_manual_review_candidate(row):
                    continue
                review_key = _build_reconciliation_review_key(
                    company=company,
                    guid=row.get("guid"),
                    device_id=row.get("device_id"),
                    plate_no=row.get("plate_no"),
                    raw_alarm_type=row.get("raw_alarm_type"),
                    raw_tp=row.get("raw_tp"),
                    raw_event_code=row.get("raw_event_code"),
                    portal_begin_time=row.get("portal_begin_time"),
                    portal_reporting_time=row.get("portal_reporting_time"),
                )
                portal_payload = payload_by_key.get(review_key)
                if not portal_payload:
                    continue
                review = session.scalar(
                    select(ReconciliationReview).where(ReconciliationReview.review_key == review_key)
                )
                if not review:
                    review = ReconciliationReview(
                        review_key=review_key,
                        company_slug=company.slug,
                        review_status="pending",
                    )
                review.guid = row.get("guid")
                review.device_id = row.get("device_id")
                review.plate_no = row.get("plate_no")
                review.observed_at=ensure_utc(row.get("observed_at"))
                review.portal_begin_time = row.get("portal_begin_time")
                review.portal_reporting_time = row.get("portal_reporting_time")
                review.raw_alarm_type = row.get("raw_alarm_type")
                review.raw_tp = row.get("raw_tp")
                review.raw_event_code = row.get("raw_event_code")
                review.classification_status = row.get("classification_status")
                review.visibility_status = row.get("visibility_status")
                review.category = row.get("category")
                review.subtype = row.get("subtype")
                review.reason = row.get("reason") or "missing_local"
                review.diagnostic_note = row.get("diagnostic_note")
                review.suggested_action = "reconcile"
                review.source_job_id = job_id
                review.source_window_type = window_type
                review.portal_payload_json = json.dumps(portal_payload, ensure_ascii=True)
                session.add(review)
            session.commit()

    def _sync_portal_rows(
        self,
        *,
        company: CompanyConfig,
        portal_rows: list[dict[str, Any]],
        force_temporal_accept: bool = False,
    ) -> None:
        now = utc_now()
        with self.session_factory() as session:
            snapshot_cache: dict[tuple[str, date], DailyMileageSnapshot | None] = {}
            for portal_row in portal_rows:
                normalized = self.howen.normalize_alarm(portal_row)
                if not normalized:
                    continue
                occurred_at = ensure_utc(normalized.occurred_at) or normalized.occurred_at
                temporal_valid = force_temporal_accept or not _is_future_event(
                    normalized.occurred_at,
                    tolerance_minutes=self.settings.anomaly_future_tolerance_minutes,
                )
                device = session.get(DeviceRecord, normalized.device_id) or DeviceRecord(device_id=normalized.device_id)
                device.plate_no = normalized.plate_no or device.plate_no
                device.company_slug = company.slug
                device.fleet_id = normalized.fleet_id or device.fleet_id
                device.driver_name = normalized.driver_name or device.driver_name
                device.last_seen_at = _max_or_value(device.last_seen_at, occurred_at)
                if normalized.total_mileage_km is not None:
                    device.last_total_km = normalized.total_mileage_km
                session.add(device)
                effective_fleet_id = normalized.fleet_id or device.fleet_id
                effective_plate = normalized.plate_no or device.plate_no
                raw_row = session.get(HowenAlarmRaw, normalized.guid) or HowenAlarmRaw(guid=normalized.guid)
                raw_row.company_slug = company.slug
                raw_row.device_id = normalized.device_id
                raw_row.fleet_id = effective_fleet_id
                raw_row.plate_no = effective_plate
                raw_row.source = raw_row.source or "backfill"
                raw_row.occurred_at = occurred_at
                raw_row.received_at = _max_or_value(raw_row.received_at, now) or now
                raw_row.raw_alarm_type = normalized.raw_alarm_type
                raw_row.raw_tp = normalized.raw_tp
                raw_row.raw_event_code = normalized.raw_event_code
                raw_row.raw_event_time = normalized.raw_event_time
                raw_row.classification_status = normalized.classification_status
                raw_row.mapped_category = normalized.category
                raw_row.mapping_source = normalized.mapping_source
                raw_row.temporal_status = "accepted" if temporal_valid else "future_rejected"
                raw_row.payload_json = json.dumps(normalized.raw, ensure_ascii=True)

                audit_reason = None
                if not temporal_valid:
                    raw_row.ingest_result = "future_rejected"
                    audit_reason = "future_rejected"
                elif normalized.classification_status != "classified_dms":
                    raw_row.ingest_result = "kept_raw_only_non_dms" if normalized.classification_status == "classified_non_dms" else "kept_raw_only_unmapped"
                    audit_reason = normalized.classification_status
                else:
                    existing = session.get(AlarmEvent, normalized.guid)
                    if not existing:
                        existing = AlarmEvent(
                            guid=normalized.guid,
                            device_id=normalized.device_id,
                            occurred_at=occurred_at,
                            source="backfill",
                        )
                        audit_reason = "inserted_from_portal"
                        session.add(existing)
                    elif (
                        existing.category != normalized.category
                        or existing.classification_status != normalized.classification_status
                        or existing.subtype != normalized.subtype
                    ):
                        audit_reason = "updated_from_portal"

                    existing.plate_no = effective_plate or existing.plate_no
                    existing.company_slug = company.slug
                    existing.fleet_id = effective_fleet_id or existing.fleet_id
                    existing.driver_name = normalized.driver_name or existing.driver_name
                    existing.category = normalized.category
                    existing.subtype = normalized.subtype
                    existing.mapping_source = normalized.mapping_source
                    existing.classification_status = normalized.classification_status
                    existing.visibility_status = normalized.visibility_status
                    existing.event_code = normalized.event_code
                    existing.raw_alarm_type = normalized.raw_alarm_type
                    existing.raw_tp = normalized.raw_tp
                    existing.raw_event_code = normalized.raw_event_code
                    existing.occurred_at = occurred_at
                    existing.received_at = now
                    existing.start_at = normalized.start_at
                    existing.end_at = normalized.end_at
                    existing.raw_event_time = normalized.raw_event_time
                    existing.latitude = normalized.latitude
                    existing.longitude = normalized.longitude
                    existing.total_mileage_km = normalized.total_mileage_km
                    existing.raw_payload = json.dumps(normalized.raw, ensure_ascii=True)
                    raw_row.ingest_result = "inserted_alarm_event" if audit_reason == "inserted_from_portal" else "updated_alarm_event"

                    snapshot_date = occurred_at.astimezone(ZoneInfo(company.timezone or self.settings.default_timezone)).date()
                    if normalized.total_mileage_km is not None:
                        snapshot_key = (normalized.device_id, snapshot_date)
                        snapshot = snapshot_cache.get(snapshot_key)
                        if snapshot_key not in snapshot_cache:
                            snapshot = session.scalar(
                                select(DailyMileageSnapshot).where(
                                    DailyMileageSnapshot.device_id == normalized.device_id,
                                    DailyMileageSnapshot.snapshot_date == snapshot_date,
                                )
                            )
                            snapshot_cache[snapshot_key] = snapshot
                        if not snapshot:
                            snapshot = DailyMileageSnapshot(
                                device_id=normalized.device_id,
                                plate_no=effective_plate,
                                company_slug=company.slug,
                                fleet_id=effective_fleet_id,
                                snapshot_date=snapshot_date,
                                total_km=normalized.total_mileage_km,
                                day_km=None,
                                raw_total_value=_string_or_none(_nested_value(normalized.raw, "totalMileage") or _nested_value(normalized.raw, "total")),
                                source="backfill",
                                observed_at=occurred_at,
                            )
                            snapshot_cache[snapshot_key] = snapshot
                        else:
                            if occurred_at >= (ensure_utc(snapshot.observed_at) or occurred_at):
                                snapshot.observed_at = occurred_at
                                snapshot.total_km = normalized.total_mileage_km
                            snapshot.plate_no = effective_plate or snapshot.plate_no
                            snapshot.company_slug = company.slug
                            snapshot.fleet_id = effective_fleet_id or snapshot.fleet_id
                            snapshot.raw_total_value = _string_or_none(
                                _nested_value(normalized.raw, "totalMileage") or _nested_value(normalized.raw, "total")
                            )
                        session.add(snapshot)
                session.add(raw_row)

                if audit_reason:
                    _append_dashboard_audit(
                        session,
                        guid=normalized.guid,
                        company_slug=company.slug,
                        device_id=normalized.device_id,
                        fleet_id=effective_fleet_id,
                        plate_no=effective_plate,
                        observed_at=occurred_at,
                        received_at=now,
                        raw_alarm_type=normalized.raw_alarm_type,
                        raw_tp=normalized.raw_tp,
                        raw_event_code=normalized.raw_event_code,
                        stage="reconciliation",
                        reason=audit_reason,
                        payload=normalized.raw,
                    )
            session.commit()

    def list_vehicle_status(self, company_slug: str) -> list[dict[str, Any]]:
        company = self.registry.get(company_slug)
        tz = ZoneInfo(company.timezone or self.settings.default_timezone)
        snapshot_date = utc_now().astimezone(tz).date()
        with self.session_factory() as session:
            devices = [
                device
                for device in session.scalars(select(DeviceRecord).where(DeviceRecord.record_source == "live").order_by(DeviceRecord.plate_no))
                if self.registry.device_belongs(company, device.device_id, device.fleet_id)
            ]
            snapshots = list(
                session.scalars(
                    select(DailyMileageSnapshot)
                    .where(DailyMileageSnapshot.source.in_(ACTIVE_SNAPSHOT_SOURCES), DailyMileageSnapshot.snapshot_date == snapshot_date)
                    .order_by(DailyMileageSnapshot.observed_at.desc())
                )
            )
            alarms = list(
                session.scalars(
                    select(HowenAlarmRaw)
                    .where(HowenAlarmRaw.received_at >= utc_now() - timedelta(days=7))
                    .order_by(HowenAlarmRaw.received_at.desc())
                )
            )

        latest_snapshot_by_device: dict[str, DailyMileageSnapshot] = {}
        for snapshot_row in snapshots:
            if not self.registry.device_belongs(company, snapshot_row.device_id, snapshot_row.fleet_id):
                continue
            latest_snapshot_by_device.setdefault(snapshot_row.device_id, snapshot_row)

        last_alarm_by_device: dict[str, datetime] = {}
        for alarm in alarms:
            if self.registry.device_belongs(company, alarm.device_id, alarm.fleet_id) and alarm.device_id not in last_alarm_by_device:
                last_alarm_by_device[alarm.device_id] = ensure_utc(alarm.occurred_at) or ensure_utc(alarm.received_at) or alarm.received_at

        rows = []
        for device in devices:
            last_received_at = ensure_utc(device.last_received_at)
            last_seen_at = ensure_utc(device.last_seen_at)
            snapshot_row = latest_snapshot_by_device.get(device.device_id)
            if snapshot_row:
                snapshot_row.observed_at = ensure_utc(snapshot_row.observed_at) or snapshot_row.observed_at
            rows.append(
                AdminVehicleView(
                    device_id=device.device_id,
                    plate_no=device.plate_no,
                    fleet_id=device.fleet_id,
                    fleet_name=device.fleet_name,
                    device_name=device.device_name,
                    driver_name=device.driver_name,
                    last_received_at=last_received_at,
                    last_seen_at=last_seen_at,
                    last_alarm_at=last_alarm_by_device.get(device.device_id),
                    last_total_km=device.last_total_km,
                    last_day_km=device.last_day_km,
                    last_snapshot_total_km=snapshot_row.total_km if snapshot_row else None,
                    last_snapshot_day_km=snapshot_row.day_km if snapshot_row else None,
                    last_snapshot_at=snapshot_row.observed_at if snapshot_row else None,
                    feed_status=_vehicle_feed_status(last_received_at, company, tz),
                    record_source=device.record_source,
                ).model_dump(mode="json")
            )
        rows.sort(key=lambda row: (row["feed_status"], row["plate_no"] or row["device_id"]))
        return rows

    def list_anomalies(
        self,
        *,
        company_slug: str | None = None,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        with self.session_factory() as session:
            query = select(IngestionAnomaly).order_by(IngestionAnomaly.received_at.desc())
            if company_slug:
                query = query.where(IngestionAnomaly.company_slug == company_slug)
            if start_at:
                query = query.where(IngestionAnomaly.received_at >= ensure_utc(start_at))
            if end_at:
                query = query.where(IngestionAnomaly.received_at <= ensure_utc(end_at))
            query = query.limit(limit)
            anomalies = list(session.scalars(query))
        for anomaly in anomalies:
            anomaly.received_at = ensure_utc(anomaly.received_at) or anomaly.received_at
        return [
            {
                "id": anomaly.id,
                "source_type": anomaly.source_type,
                "device_id": anomaly.device_id,
                "company_slug": anomaly.company_slug,
                "received_at": anomaly.received_at.isoformat(),
                "raw_event_time": anomaly.raw_event_time,
                "reason": anomaly.reason,
                "payload_json": anomaly.payload_json,
            }
            for anomaly in anomalies
        ]

    def list_raw_alarm_diagnostics(
        self,
        *,
        company_slug: str,
        limit: int = 100,
        source: str | None = None,
        classification_status: str | None = None,
        only_problematic: bool = True,
    ) -> list[dict[str, Any]]:
        company = self.registry.get(company_slug)
        with self.session_factory() as session:
            query = select(HowenAlarmRaw).order_by(HowenAlarmRaw.received_at.desc())
            if source:
                query = query.where(HowenAlarmRaw.source == source)
            if classification_status:
                query = query.where(HowenAlarmRaw.classification_status == classification_status)
            if only_problematic:
                query = query.where(
                    or_(
                        HowenAlarmRaw.classification_status != "classified_dms",
                        HowenAlarmRaw.temporal_status != "accepted",
                        HowenAlarmRaw.ingest_result.is_(None),
                        ~HowenAlarmRaw.ingest_result.in_(("inserted_alarm_event", "updated_alarm_event")),
                    )
                )
            raw_rows = session.scalars(query)
        filtered: list[dict[str, Any]] = []
        for row in raw_rows:
            if not self.registry.device_belongs(company, row.device_id, row.fleet_id):
                continue
            filtered.append(_serialize_raw_alarm_diagnostic(row))
            if len(filtered) >= limit:
                break
        return filtered

    def _load_raw_alarm_metrics(
        self,
        session,
        *,
        cutoff: datetime,
        company: CompanyConfig | None = None,
        company_slug: str | None = None,
    ) -> dict[str, Any]:
        query = select(HowenAlarmRaw).where(HowenAlarmRaw.received_at >= cutoff).order_by(HowenAlarmRaw.received_at.desc())
        raw_rows = list(session.scalars(query))
        if company:
            filtered = [
                row
                for row in raw_rows
                if self.registry.device_belongs(company, row.device_id, row.fleet_id)
            ]
        elif company_slug:
            filtered = [row for row in raw_rows if row.company_slug == company_slug]
        else:
            filtered = raw_rows

        return {
            "raw_rows": filtered,
            "live_alarm_count_24h": sum(1 for row in filtered if row.source == "live"),
            "raw_dms_count_24h": sum(
                1 for row in filtered if row.classification_status == "classified_dms" and row.temporal_status == "accepted"
            ),
            "live_dms_count_24h": sum(
                1
                for row in filtered
                if row.source == "live" and row.classification_status == "classified_dms" and row.temporal_status == "accepted"
            ),
            "backfill_dms_count_24h": sum(
                1
                for row in filtered
                if row.source == "backfill" and row.classification_status == "classified_dms" and row.temporal_status == "accepted"
            ),
            "catchup_dms_count_24h": sum(
                1
                for row in filtered
                if row.source == "catchup" and row.classification_status == "classified_dms" and row.temporal_status == "accepted"
            ),
            "live_unmapped_count_24h": sum(
                1 for row in filtered if row.source == "live" and row.classification_status == "unmapped"
            ),
            "non_dms_count_24h": sum(
                1 for row in filtered if row.classification_status == "classified_non_dms" and row.temporal_status == "accepted"
            ),
            "live_non_dms_count_24h": sum(
                1
                for row in filtered
                if row.source == "live" and row.classification_status == "classified_non_dms" and row.temporal_status == "accepted"
            ),
            "future_rejected_count_24h": sum(1 for row in filtered if row.temporal_status == "future_rejected"),
            "live_future_rejected_count_24h": sum(
                1 for row in filtered if row.source == "live" and row.temporal_status == "future_rejected"
            ),
            "vehicles_with_any_alarm_24h": len({row.device_id for row in filtered if row.device_id}),
            "vehicles_with_live_dms_24h": len(
                {
                    row.device_id
                    for row in filtered
                    if row.device_id and row.source == "live" and row.classification_status == "classified_dms" and row.temporal_status == "accepted"
                }
            ),
        }

    def _build_feed_state(self, *, state: IngestState | None, company: CompanyConfig, now_local: datetime) -> dict[str, Any]:
        if not state or not state.last_cycle_received_at:
            return FeedState(
                status="sin_datos",
                label="sin datos aun",
                connection_state=state.connection_state if state else "idle",
                last_error=state.last_error if state else None,
            ).model_dump(mode="json")

        local_last = as_timezone(state.last_cycle_received_at, company.timezone)
        age = int(max((now_local - local_last).total_seconds(), 0) // 60) if local_last else None
        if age is None:
            status = "sin_datos"
            label = "sin datos aun"
        elif age < company.rules.feed_late_threshold_minutes:
            status = "al_dia"
            label = f"datos hasta {local_last.strftime('%H:%M')} · hace {age} min"
        elif age < company.rules.feed_stopped_threshold_minutes:
            status = "atrasado"
            label = f"sin datos nuevos hace {age} min"
        else:
            status = "detenido"
            label = f"feed detenido · {local_last.strftime('%H:%M')}"
        return FeedState(
            status=status,
            label=label,
            minutes_since_last_message=age,
            last_message_at=state.last_message_at,
            last_cycle_received_at=state.last_cycle_received_at,
            last_event_observed_at=state.last_event_observed_at,
            last_alarm_at=state.last_alarm_at,
            last_status_at=state.last_status_at,
            last_live_alarm_message_at=state.last_live_alarm_message_at,
            last_live_dms_at=state.last_live_dms_at,
            last_live_unmapped_at=state.last_live_unmapped_at,
            connection_state=state.connection_state,
            last_error=state.last_error,
        ).model_dump(mode="json")


def _build_daily_km(
    daily_snapshots: list[DailyMileageSnapshot],
    legacy_mileages: list[MileageReading],
    alarm_events: list[AlarmEvent],
    tz: ZoneInfo,
    *,
    legacy_daily_km: list[tuple[str, date, float]] | None = None,
) -> tuple[dict[str, dict[date, float]], dict[date, float]]:
    grouped: dict[str, dict[date, float]] = defaultdict(dict)
    fleet_by_date: dict[date, float] = defaultdict(float)
    latest_by_plate_day: dict[tuple[str, date], DailyMileageSnapshot] = {}
    for snapshot in daily_snapshots:
        plate = snapshot.plate_no or snapshot.device_id
        day_key = snapshot.snapshot_date
        existing = latest_by_plate_day.get((plate, day_key))
        if not existing or snapshot.observed_at > existing.observed_at:
            latest_by_plate_day[(plate, day_key)] = snapshot

    by_plate: dict[str, list[tuple[date, DailyMileageSnapshot]]] = defaultdict(list)
    for (plate, day_key), snapshot in latest_by_plate_day.items():
        by_plate[plate].append((day_key, snapshot))

    for plate, entries in by_plate.items():
        entries.sort(key=lambda item: item[0])
        previous_total: float | None = None
        for day_key, snapshot in entries:
            total_km = round(max(snapshot.total_km or 0.0, 0.0), 1) if snapshot.total_km is not None else None
            km = _sanitize_day_km_value(snapshot.day_km, total_km, previous_total)
            if km is None and total_km is not None and previous_total is not None:
                km = round(max(total_km - previous_total, 0.0), 1)
            elif km is None:
                km = 0.0
            _merge_daily_km_value(grouped, fleet_by_date, plate, day_key, km, replace=True)
            previous_total = total_km if total_km is not None else previous_total

    raw_grouped: dict[str, dict[date, list[MileageReading]]] = defaultdict(lambda: defaultdict(list))
    for reading in legacy_mileages:
        plate = reading.plate_no or reading.device_id
        day_key = ensure_utc(reading.recorded_at).astimezone(tz).date()
        raw_grouped[plate][day_key].append(reading)
    for plate, days in raw_grouped.items():
        for day_key, rows in days.items():
            rows.sort(key=lambda row: row.recorded_at)
            explicit = [row.day_km for row in rows if row.day_km is not None]
            if explicit:
                km = max(explicit)
            else:
                km = max(rows[-1].total_km - rows[0].total_km, 0.0)
            _merge_daily_km_value(grouped, fleet_by_date, plate, day_key, km)

    for plate, day_key, km in legacy_daily_km or []:
        _merge_daily_km_value(grouped, fleet_by_date, plate, day_key, km)

    # Alarm payloads are sparse observations, not an odometer series. Using
    # their min/max as daily distance creates misleading partial history.
    # Historical km is published only from status readings or daily snapshots.
    return grouped, fleet_by_date


def _load_legacy_daily_km(
    session: Any,
    *,
    company: CompanyConfig,
    cutoff: datetime,
    reference_utc: datetime,
) -> list[tuple[str, str | None, date, float]]:
    """Aggregate status observations in SQL instead of loading every sample."""
    dialect_name = session.get_bind().dialect.name
    if dialect_name == "postgresql":
        day_expression = func.date(func.timezone(company.timezone, MileageReading.recorded_at))
    else:
        day_expression = func.date(MileageReading.recorded_at)
    partition = (MileageReading.device_id, day_expression)
    ranked = (
        select(
            MileageReading.device_id.label("device_id"),
            MileageReading.plate_no.label("plate_no"),
            day_expression.label("day_key"),
            MileageReading.total_km.label("total_km"),
            MileageReading.day_km.label("day_km"),
            func.row_number()
            .over(partition_by=partition, order_by=MileageReading.recorded_at.asc())
            .label("first_rank"),
            func.row_number()
            .over(partition_by=partition, order_by=MileageReading.recorded_at.desc())
            .label("last_rank"),
        )
        .where(
            _company_membership_clause(MileageReading, company),
            MileageReading.source.in_(ACTIVE_MILEAGE_SOURCES),
            MileageReading.recorded_at >= cutoff,
            MileageReading.recorded_at <= reference_utc,
        )
        .subquery()
    )
    rows = session.execute(
        select(
            ranked.c.device_id,
            func.max(ranked.c.plate_no).label("plate_no"),
            ranked.c.day_key,
            func.max(ranked.c.day_km).label("explicit_day_km"),
            func.max(case((ranked.c.first_rank == 1, ranked.c.total_km))).label("first_total_km"),
            func.max(case((ranked.c.last_rank == 1, ranked.c.total_km))).label("last_total_km"),
        ).group_by(ranked.c.device_id, ranked.c.day_key)
    ).all()
    aggregates: list[tuple[str, str | None, date, float]] = []
    for row in rows:
        day_key = row.day_key if isinstance(row.day_key, date) else date.fromisoformat(str(row.day_key))
        if row.explicit_day_km is not None:
            day_km = float(row.explicit_day_km)
        elif row.first_total_km is not None and row.last_total_km is not None:
            day_km = max(float(row.last_total_km) - float(row.first_total_km), 0.0)
        else:
            continue
        aggregates.append((str(row.device_id), row.plate_no, day_key, round(day_km, 1)))
    return aggregates


def _merge_daily_km_value(
    grouped: dict[str, dict[date, float]],
    fleet_by_date: dict[date, float],
    plate: str,
    day_key: date,
    km: float | None,
    *,
    replace: bool = False,
) -> None:
    if km is None:
        return
    next_value = round(max(km, 0.0), 1)
    current_value = grouped[plate].get(day_key)
    if current_value is None:
        grouped[plate][day_key] = next_value
        fleet_by_date[day_key] += next_value
        return
    if replace and next_value != current_value:
        grouped[plate][day_key] = next_value
        fleet_by_date[day_key] += next_value - current_value
        return
    if next_value > current_value:
        grouped[plate][day_key] = next_value
        fleet_by_date[day_key] += next_value - current_value


def _normalize_persisted_alarm_km(value: float | None) -> float | None:
    if value is None:
        return None
    normalized = float(value)
    if normalized >= 100_000:
        normalized /= 1000
    elif normalized >= 10_000:
        normalized /= 1000
    return round(max(normalized, 0.0), 1)


def _build_baselines(
    *,
    baseline_dates: list[date],
    daily_km_by_vehicle: dict[str, dict[date, float]],
    events_by_vehicle_day: dict[str, Counter[date]],
) -> dict[str, float]:
    baselines = {}
    for plate, km_days in daily_km_by_vehicle.items():
        samples = [
            events_by_vehicle_day.get(plate, Counter()).get(day_key, 0)
            for day_key in baseline_dates
            if km_days.get(day_key, 0) > 0
        ]
        baselines[plate] = round(mean(samples), 2) if samples else 0.0
    return baselines


def _merge_current_day_from_device_state(
    devices: list[DeviceRecord],
    daily_km_by_vehicle: dict[str, dict[date, float]],
    fleet_km_by_date: dict[date, float],
    latest_day: date,
    tz: ZoneInfo,
) -> None:
    for device in devices:
        reference_time = device.last_received_at or device.last_seen_at
        if not reference_time or reference_time.astimezone(tz).date() != latest_day:
            continue
        next_value = _sanitize_day_km_value(device.last_day_km, device.last_total_km, None)
        if next_value is None:
            continue
        plate = device.plate_no or device.device_id
        current_value = daily_km_by_vehicle[plate].get(latest_day)
        if current_value is None:
            daily_km_by_vehicle[plate][latest_day] = next_value
            fleet_km_by_date[latest_day] += next_value
            continue
        if round(current_value, 1) == round(next_value, 1):
            continue
        daily_km_by_vehicle[plate][latest_day] = next_value
        fleet_km_by_date[latest_day] += next_value - current_value


def _build_recent_episode_metrics(
    events: list[AlarmEvent],
    company: CompanyConfig,
    tz: ZoneInfo,
    daily_km_by_vehicle: dict[str, dict[date, float]],
    review_status_by_guid: dict[str, str] | None = None,
) -> dict[str, int]:
    return dict(
        _build_recent_episode_analysis(
            events,
            company,
            tz,
            daily_km_by_vehicle,
            review_status_by_guid=review_status_by_guid,
        )["metrics"]
    )


def _build_recent_episode_analysis(
    events: list[AlarmEvent],
    company: CompanyConfig,
    tz: ZoneInfo,
    daily_km_by_vehicle: dict[str, dict[date, float]],
    review_status_by_guid: dict[str, str] | None = None,
    fleet_vehicle_count: int | None = None,
) -> dict[str, Any]:
    if not events:
        return {
            "metrics": {
                "raw_events": 0,
                "grouped_episodes": 0,
                "visible_alerts": 0,
                "fused_in_episode": 0,
                "dismissed_alerts": 0,
                "suppressed_by_rule": 0,
                "visible_raw_events": 0,
                "non_dms_hidden": 0,
                "unmapped_hidden": 0,
                "future_rejected": 0,
            },
            "guid_status": {},
            "episodes": [],
        }

    review_status_by_guid = review_status_by_guid or {}
    approved_guids = {guid for guid, status in review_status_by_guid.items() if status == "approved"}
    raw_events = sorted(
        [event for event in events if review_status_by_guid.get(event.guid) != "discarded"],
        key=lambda event: event.occurred_at.astimezone(tz),
    )
    event_local_dt = {event.guid: event.occurred_at.astimezone(tz) for event in raw_events}
    event_local_date = {guid: value.date() for guid, value in event_local_dt.items()}
    guid_status: dict[str, dict[str, Any]] = {}

    # N2: only the first five daytime eye-closed detections per vehicle/day flow
    # automatically. Later detections remain auditable and require a human decision.
    daytime_eye_counts: Counter[tuple[str, date]] = Counter()
    rule_events: list[AlarmEvent] = []
    suppressed_eye_events = 0
    for event in raw_events:
        local_dt = event_local_dt[event.guid]
        plate = event.plate_no or event.device_id
        if (
            event.category == "Ojos cerrados"
            and company.rules.eyes_closed_daytime_start_hour
            <= local_dt.hour
            < company.rules.eyes_closed_daytime_end_hour
        ):
            eye_key = (plate, local_dt.date())
            daytime_eye_counts[eye_key] += 1
            if (
                daytime_eye_counts[eye_key] > company.rules.eyes_closed_daytime_review_threshold
                and event.guid not in approved_guids
            ):
                suppressed_eye_events += 1
                guid_status[event.guid] = {
                    "visibility_status": "suppressed_by_rule",
                    "reason": "eyes_closed_daytime_limit",
                    "episode_guid": None,
                    "episode_title": None,
                }
                continue
        rule_events.append(event)

    grouped: list[dict[str, Any]] = []
    open_groups: dict[str, dict[str, Any]] = {}
    for event in rule_events:
        local_dt = event_local_dt[event.guid]
        plate = event.plate_no or event.device_id
        if event.category == "Fumando":
            is_night_shift = _is_night(
                local_dt.hour,
                company.rules.night_window_start,
                company.rules.night_window_end,
            )
            shift_day = (
                local_dt.date() - timedelta(days=1)
                if is_night_shift and local_dt.hour < company.rules.night_window_end
                else local_dt.date()
            )
            shift_key = f"{'night' if is_night_shift else 'day'}:{shift_day.isoformat()}"
            key = f"{plate}|{event.category}|{shift_key}"
        else:
            key = f"{plate}|{event.category}"
        current = open_groups.get(key)
        event_dt = event_local_dt[event.guid]
        gap_minutes = (
            (event_dt - event_local_dt[current["events"][-1].guid]).total_seconds() / 60 if current else None
        )
        window_minutes = (
            company.rules.collision_window_minutes
            if event.category == "Riesgo de colision"
            else company.rules.yawn_window_minutes
            if event.category == "Bostezo"
            else company.rules.streak_window_minutes
        )

        if current and (
            event.category == "Fumando"
            or (gap_minutes is not None and gap_minutes <= window_minutes)
        ):
            current["events"].append(event)
            continue

        next_group = {
            "plate": plate,
            "category": event.category,
            "events": [event],
            "group_id": (plate, event.category, event.guid),
        }
        open_groups[key] = next_group
        grouped.append(next_group)

    visible = 0
    dismissed = suppressed_eye_events
    suppressed_raw = suppressed_eye_events
    visible_raw = 0
    episodes: list[dict[str, Any]] = []
    company_events_by_vehicle_day = defaultdict(Counter)
    same_day_category_counts: Counter[tuple[str, str, date]] = Counter()
    for event in rule_events:
        plate = event.plate_no or event.device_id
        day_key = event_local_date[event.guid]
        company_events_by_vehicle_day[plate][day_key] += 1
        same_day_category_counts[(plate, event.category, day_key)] += 1
    effective_fleet_size = max(
        fleet_vehicle_count or 0,
        len({event.plate_no or event.device_id for event in rule_events}),
        len(daily_km_by_vehicle),
        1,
    )
    distraction_totals_by_day = Counter(
        event_local_date[event.guid]
        for event in rule_events
        if event.category == "Distraccion"
    )
    camera_days_by_plate: dict[str, set[date]] = defaultdict(set)
    for event in rule_events:
        if event.category == "Camara cubierta":
            camera_days_by_plate[event.plate_no or event.device_id].add(event_local_date[event.guid])

    # Reserve the most recent yawning episode before an eye-closed episode so the
    # client receives one fatigue episode instead of two duplicated cards.
    yawn_group_for_eye: dict[tuple[str, str, str], dict[str, Any]] = {}
    reserved_yawn_groups: set[tuple[str, str, str]] = set()
    yawn_groups_by_plate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for group in grouped:
        if group["category"] == "Bostezo":
            yawn_groups_by_plate[group["plate"]].append(group)
            continue
        if group["category"] != "Ojos cerrados":
            continue
        first_eye_dt = event_local_dt[group["events"][0].guid]
        for candidate in reversed(yawn_groups_by_plate.get(group["plate"], [])):
            if candidate["group_id"] in reserved_yawn_groups:
                continue
            candidate_last_dt = event_local_dt[candidate["events"][-1].guid]
            if candidate_last_dt >= first_eye_dt:
                continue
            if (
                first_eye_dt - candidate_last_dt
            ).total_seconds() / 60 <= company.rules.fatigue_merge_window_minutes:
                yawn_group_for_eye[group["group_id"]] = candidate
                reserved_yawn_groups.add(candidate["group_id"])
                break

    for group in grouped:
        first = group["events"][0]
        last = group["events"][-1]
        plate = group["plate"]
        group_key = group["group_id"]
        if group_key in reserved_yawn_groups:
            continue

        last_local_dt = event_local_dt[last.guid]
        first_local_dt = event_local_dt[first.guid]
        same_day_count = same_day_category_counts[(plate, group["category"], event_local_date[last.guid])]
        current_category = group["category"]
        reason = "visible"
        merged_groups = [group]
        episode_title = current_category
        episode_level = "medio"

        if group["category"] == "Ojos cerrados":
            matching_yawn = yawn_group_for_eye.get(group_key)
            if matching_yawn:
                merged_groups.append(matching_yawn)
                current_category = "Fatiga en progresion"
                reason = "merged_yawn_into_fatigue"
                episode_title = "Fatiga en progresion"
                episode_level = "critico"
            else:
                episode_title = "Ojos cerrados"
                episode_level = (
                    "critico"
                    if len(group["events"]) >= company.rules.eyes_closed_critical_threshold
                    else "alto"
                )
                if any(event.guid in approved_guids for event in group["events"]):
                    reason = "manual_approved"
        elif group["category"] == "Distraccion":
            episode_title = "Distraccion"
            if any(event.guid in approved_guids for event in group["events"]):
                reason = "manual_approved"
            else:
                day_total = distraction_totals_by_day[event_local_date[last.guid]]
                fleet_average = day_total / effective_fleet_size
                above_fleet_threshold = same_day_count > fleet_average * 3
                if above_fleet_threshold:
                    reason = "distraction_above_3x_fleet_average"
                else:
                    dismissed += 1
                    suppressed_raw += len(group["events"])
                    for event in group["events"]:
                        guid_status[event.guid] = {
                            "visibility_status": "suppressed_by_rule",
                            "reason": "distraction_below_3x_fleet_average",
                            "episode_guid": None,
                            "episode_title": None,
                        }
                    continue
        elif group["category"] == "Uso de celular":
            episode_title = "Uso de celular"
            episode_level = "critico"
        elif group["category"] == "Riesgo de colision":
            episode_title = "Riesgo de colision"
            episode_level = (
                "alto"
                if len(group["events"]) >= company.rules.collision_pattern_threshold
                else "medio"
            )
        elif group["category"] == "Bostezo":
            episode_title = "Bostezo"
            episode_level = (
                "alto"
                if len(group["events"]) >= company.rules.yawn_fatigue_threshold
                else "medio"
            )
        elif group["category"] == "Camara cubierta":
            episode_title = "Camara cubierta"
            event_day = event_local_date[last.guid]
            episode_level = (
                "alto"
                if event_day - timedelta(days=1) in camera_days_by_plate.get(plate, set())
                else "medio"
            )
            if episode_level == "alto":
                reason = "camera_covered_consecutive_days"
        elif group["category"] == "Fumando":
            episode_title = "Fumando"
            reason = "smoking_grouped_by_shift"

        merged_events = sorted(
            [event for merged_group in merged_groups for event in merged_group["events"]],
            key=lambda event: event.occurred_at.astimezone(tz),
        )
        visible += 1
        visible_raw += len(merged_events)
        episode_guid = merged_events[0].guid
        episodes.append(
            {
                "episode_guid": episode_guid,
                "episode_title": episode_title,
                "category": current_category,
                "level": episode_level,
                "reason": reason,
                "plate": plate,
                "started_at": first_local_dt.isoformat(),
                "ended_at": last_local_dt.isoformat(),
                "guid_count": len(merged_events),
                "raw_guids": [event.guid for event in merged_events],
            }
        )
        for index, event in enumerate(merged_events):
            guid_status[event.guid] = {
                "visibility_status": "visible_episode" if index == 0 else "fused_in_episode",
                "reason": reason if index == 0 else "fused_in_episode",
                "episode_guid": episode_guid,
                "episode_title": episode_title,
                "episode_level": episode_level,
            }

    return {
        "metrics": {
            "raw_events": len(raw_events),
            "grouped_episodes": len(grouped),
            "visible_alerts": visible,
            "fused_in_episode": max(visible_raw - visible, 0),
            "dismissed_alerts": dismissed,
            "suppressed_by_rule": suppressed_raw,
            "visible_raw_events": visible_raw,
            "non_dms_hidden": 0,
            "unmapped_hidden": 0,
            "future_rejected": 0,
        },
        "guid_status": guid_status,
        "episodes": episodes,
    }


def _serialize_event(
    event: AlarmEvent,
    tz: ZoneInfo,
    rule_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "guid": event.guid,
        "deviceId": event.device_id,
        "plate": event.plate_no or event.device_id,
        "category": event.category,
        "subtype": event.subtype,
        "occurredAt": event.occurred_at.astimezone(tz).isoformat(),
        "driverName": event.driver_name,
        "latitude": event.latitude,
        "longitude": event.longitude,
        "totalMileageKm": event.total_mileage_km,
    }
    if rule_status:
        payload.update(
            {
                "episodeGuid": rule_status.get("episode_guid"),
                "episodeTitle": rule_status.get("episode_title"),
                "ruleLevel": rule_status.get("episode_level"),
                "ruleReason": rule_status.get("reason"),
            }
        )
    return payload


def _build_publication_state(*, company: CompanyConfig, settings: Any) -> dict[str, Any]:
    dashboard_url = settings.public_dashboard_url or (f"https://{company.subdomain}" if company.subdomain else None)
    api_url = settings.public_api_url or (f"{dashboard_url.rstrip('/')}/api" if dashboard_url else None)
    host = _extract_host(dashboard_url or company.subdomain)
    if not host:
        return {
            "dashboard_host": None,
            "dashboard_url": dashboard_url,
            "api_url": api_url,
            "dns_status": "unconfigured",
            "resolved_targets": [],
            "local_validation_only": True,
            "message": "Validacion local activa. Aun no hay un host publico configurado para este dashboard.",
        }
    try:
        resolved_targets = sorted({item[4][0] for item in socket.getaddrinfo(host, None)})
    except socket.gaierror:
        return {
            "dashboard_host": host,
            "dashboard_url": dashboard_url,
            "api_url": api_url,
            "dns_status": "unresolved",
            "resolved_targets": [],
            "local_validation_only": True,
            "message": "La publicacion sigue bloqueada: el host publico no resuelve DNS en este momento, aunque la validacion local puede continuar.",
        }
    return {
        "dashboard_host": host,
        "dashboard_url": dashboard_url,
        "api_url": api_url,
        "dns_status": "resolved",
        "resolved_targets": resolved_targets,
        "local_validation_only": True,
        "message": "El host publico resuelve DNS. La publicacion dependera solo del redeploy y del corte final de infraestructura.",
    }


def _extract_host(value: str | None) -> str | None:
    if not value:
        return None
    if "://" in value:
        return urlparse(value).hostname
    return value.strip() or None


def _date_window(end_date: date, days: int) -> list[date]:
    start = end_date - timedelta(days=days - 1)
    return [start + timedelta(days=offset) for offset in range(days)]


def _rate(value: float, km: float) -> float | None:
    if not km:
        return None
    return round((value / km) * 100, 1)


def _is_night(hour: int, start_hour: int, end_hour: int) -> bool:
    if start_hour < end_hour:
        return start_hour <= hour < end_hour
    return hour >= start_hour or hour < end_hour


def _vehicle_feed_status(last_received_at: datetime | None, company: CompanyConfig, tz: ZoneInfo) -> str:
    if not last_received_at:
        return "sin_datos"
    now_local = utc_now().astimezone(tz)
    last_local = last_received_at.astimezone(tz)
    age_minutes = int(max((now_local - last_local).total_seconds(), 0) // 60)
    if age_minutes < company.rules.feed_late_threshold_minutes:
        return "al_dia"
    if age_minutes < company.rules.feed_stopped_threshold_minutes:
        return "atrasado"
    return "detenido"


def _is_mock_device_id(device_id: str | None) -> bool:
    return bool(device_id and device_id.startswith(MOCK_DEVICE_PREFIX))


def _is_mock_fleet_id(fleet_id: str | None) -> bool:
    return bool(fleet_id and fleet_id in MOCK_FLEET_IDS)


def _is_mock_identity(device_id: str | None, fleet_id: str | None) -> bool:
    return _is_mock_device_id(device_id) or _is_mock_fleet_id(fleet_id)


def _resolve_reconciliation_range(
    *,
    company: CompanyConfig,
    start_at: datetime,
    end_at: datetime,
    window_type: str,
    fallback_timezone: str,
) -> tuple[datetime, datetime]:
    tz = ZoneInfo(company.timezone or fallback_timezone)
    start_utc = ensure_utc(start_at) or start_at.astimezone()
    end_utc = ensure_utc(end_at) or end_at.astimezone()
    if window_type == "rolling_24h":
        end_utc = end_utc
        start_utc = end_utc - timedelta(hours=24)
        return start_utc, end_utc
    if window_type == "calendar_month_local":
        start_local = start_utc.astimezone(tz).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start_local.month == 12:
            next_month = start_local.replace(year=start_local.year + 1, month=1)
        else:
            next_month = start_local.replace(month=start_local.month + 1)
        end_local = next_month - timedelta(microseconds=1)
        return start_local.astimezone(ZoneInfo("UTC")), end_local.astimezone(ZoneInfo("UTC"))
    start_local = start_utc.astimezone(tz)
    end_local = end_utc.astimezone(tz)
    return start_local.astimezone(ZoneInfo("UTC")), end_local.astimezone(ZoneInfo("UTC"))


def _parse_json(raw_value: str | None) -> dict[str, Any]:
    if not raw_value:
        return {}
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_review_status_map(session: Any, company_slug: str) -> dict[str, str]:
    rows = list(
        session.execute(
            select(ReconciliationReview.guid, ReconciliationReview.review_status).where(
                ReconciliationReview.company_slug == company_slug,
                ReconciliationReview.guid.is_not(None),
                ReconciliationReview.review_status.in_(("approved", "discarded")),
            )
        )
    )
    result: dict[str, str] = {}
    for guid, review_status in rows:
        if guid:
            result[guid] = review_status
    return result


def _company_membership_clause(model: Any, company: CompanyConfig) -> Any:
    conditions: list[Any] = []
    company_column = getattr(model, "company_slug", None)
    device_column = getattr(model, "device_id", None)
    fleet_column = getattr(model, "fleet_id", None)
    if company_column is not None:
        conditions.append(company_column == company.slug)
    if device_column is not None and company.device_ids:
        conditions.append(device_column.in_(company.device_ids))
    if fleet_column is not None and company.fleet_ids:
        conditions.append(fleet_column.in_(company.fleet_ids))
    if not conditions:
        return device_column == "__unassigned_company__"
    return or_(*conditions)


def _nested_value(payload: dict[str, Any], key: str) -> Any:
    if key in payload:
        return payload.get(key)
    for nested_key in ("payload", "mileage", "location", "det", "ext"):
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            value = _nested_value(nested, key)
            if value not in (None, ""):
                return value
    return None


def _string_or_none(value: Any) -> str | None:
    if value in (None, "", "-"):
        return None
    return str(value).strip()


def _normalize_distance_payload_value(value: Any) -> float | None:
    raw = _string_or_none(value)
    if raw is None:
        return None
    if raw.lstrip("-").isdigit():
        return round(float(raw) / 1000, 1)
    try:
        return round(float(raw.replace(",", "")), 1)
    except ValueError:
        return None


def _is_valid_day_km(day_km: float | None, total_km: float | None) -> bool:
    if day_km is None or day_km < 0:
        return False
    if day_km > 1000:
        return False
    if total_km is not None and day_km > total_km + 0.1:
        return False
    return True


def _sanitize_day_km_value(day_km: float | None, total_km: float | None, previous_total: float | None) -> float | None:
    if _is_valid_day_km(day_km, total_km):
        return round(day_km or 0.0, 1)
    if total_km is not None and previous_total is not None and total_km >= previous_total:
        derived = total_km - previous_total
        if _is_valid_day_km(derived, total_km):
            return round(derived, 1)
    return None


def _build_reconciliation_review_key(
    *,
    company: CompanyConfig,
    guid: str | None,
    device_id: str | None,
    plate_no: str | None,
    raw_alarm_type: str | None,
    raw_tp: str | None,
    raw_event_code: str | None,
    portal_begin_time: str | None,
    portal_reporting_time: str | None,
) -> str:
    normalized_plate = normalize_plate_label(plate_no) or ""
    payload = "|".join(
        [
            company.slug,
            guid or "",
            device_id or "",
            normalized_plate,
            raw_alarm_type or "",
            raw_tp or "",
            raw_event_code or "",
            portal_begin_time or "",
            portal_reporting_time or "",
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _is_manual_review_candidate(row: dict[str, Any]) -> bool:
    classification_status = str(row.get("classification_status") or "")
    visibility_status = str(row.get("visibility_status") or "")
    reason = str(row.get("reason") or "")
    if classification_status != "classified_dms":
        return False
    if visibility_status in {"missing_local", "missing_dashboard_mapping", "rejected_temporal"}:
        return True
    return reason in {"missing_local", "rejected_temporal"} or reason.startswith("stored_local_")


def _serialize_reconciliation_review(row: ReconciliationReview) -> dict[str, Any]:
    return ReconciliationReviewItemView(
        id=row.id,
        company_slug=row.company_slug,
        guid=row.guid,
        device_id=row.device_id,
        plate_no=row.plate_no,
        observed_at=ensure_utc(row.observed_at),
        portal_begin_time=row.portal_begin_time,
        portal_reporting_time=row.portal_reporting_time,
        raw_alarm_type=row.raw_alarm_type,
        raw_tp=row.raw_tp,
        raw_event_code=row.raw_event_code,
        classification_status=row.classification_status,
        visibility_status=row.visibility_status,
        category=row.category,
        subtype=row.subtype,
        reason=row.reason,
        diagnostic_note=row.diagnostic_note,
        suggested_action=row.suggested_action,
        review_status=row.review_status,
        source_job_id=row.source_job_id,
        source_window_type=row.source_window_type,
        decision_note=row.decision_note,
        decided_by=row.decided_by,
        decided_at=ensure_utc(row.decided_at),
        applied_at=ensure_utc(row.applied_at),
    ).model_dump(mode="json")


def _payload_guid(payload: dict[str, Any]) -> str | None:
    return _string_or_none(_nested_value(payload, "alarmID") or _nested_value(payload, "guid") or _nested_value(payload, "uuid"))


def _payload_plate(payload: dict[str, Any]) -> str | None:
    return normalize_plate_label(_string_or_none(_nested_value(payload, "plateNo") or _nested_value(payload, "plateno") or _nested_value(payload, "plate")))


def _payload_device(payload: dict[str, Any]) -> str | None:
    return _string_or_none(_nested_value(payload, "deviceID") or _nested_value(payload, "deviceno") or _nested_value(payload, "deviceid"))


def _payload_alarm_type(payload: dict[str, Any]) -> str | None:
    return _string_or_none(_nested_value(payload, "alarmTypeValue") or _nested_value(payload, "alarmType"))


def _payload_alarm_tp(payload: dict[str, Any]) -> str | None:
    direct = _string_or_none(_nested_value(payload, "tp"))
    if direct:
        return direct
    detail = _string_or_none(_nested_value(payload, "alarmDetail") or _nested_value(payload, "alarmvalue"))
    if not detail or "tp:" not in detail:
        return None
    digits = detail.split("tp:", 1)[1]
    result = ""
    for char in digits:
        if char.isdigit():
            result += char
            continue
        break
    return result or None


def _payload_alarm_event_code(payload: dict[str, Any]) -> str | None:
    return _string_or_none(_nested_value(payload, "ec") or _nested_value(payload, "alarmtype") or _nested_value(payload, "alarmType"))


def _payload_begin_time(payload: dict[str, Any]) -> str | None:
    return _string_or_none(
        _nested_value(payload, "endTime")
        or _nested_value(payload, "startTime")
        or _nested_value(payload, "st")
        or _nested_value(payload, "et")
    )


def _payload_reporting_time(payload: dict[str, Any]) -> str | None:
    return _string_or_none(_nested_value(payload, "reportTime"))


def _find_reporting_time_local_match(
    local_raw_candidates: dict[tuple[str | None, str | None, str | None], list[HowenAlarmRaw]],
    *,
    company: CompanyConfig,
    registry: CompanyRegistry,
    company_timezone: str,
    device_id: str | None,
    plate_no: str | None,
    raw_alarm_type: str | None,
    raw_tp: str | None,
    raw_event_code: str | None,
    reporting_time: str | None,
) -> HowenAlarmRaw | None:
    report_at = parse_timestamp(reporting_time, company_timezone)
    if report_at is None:
        return None
    canonical_plate = registry.normalize_plate(company, plate_no)

    match_keys = [
        (device_id, canonical_plate, raw_alarm_type),
        (device_id, canonical_plate, raw_tp),
        (device_id, canonical_plate, raw_event_code),
    ]
    best_match: HowenAlarmRaw | None = None
    best_delta: float | None = None
    for key in match_keys:
        if key[2] is None:
            continue
        for candidate in local_raw_candidates.get(key, []):
            candidate_at = ensure_utc(candidate.occurred_at)
            if candidate_at is None:
                continue
            delta = abs((candidate_at - report_at).total_seconds())
            if delta > 120:
                continue
            if best_delta is None or delta < best_delta:
                best_match = candidate
                best_delta = delta
    return best_match


def _serialize_raw_alarm_diagnostic(row: HowenAlarmRaw) -> dict[str, Any]:
    row.occurred_at = ensure_utc(row.occurred_at)
    row.received_at = ensure_utc(row.received_at) or row.received_at
    return RawAlarmDiagnosticView(
        guid=row.guid,
        source=row.source,
        occurred_at=row.occurred_at,
        received_at=row.received_at,
        device_id=row.device_id,
        plate_no=row.plate_no,
        raw_alarm_type=row.raw_alarm_type,
        raw_tp=row.raw_tp,
        raw_event_code=row.raw_event_code,
        classification_status=row.classification_status,
        mapped_category=row.mapped_category,
        mapping_source=row.mapping_source,
        temporal_status=row.temporal_status,
        ingest_result=row.ingest_result,
    ).model_dump(mode="json")


def _is_future_event(observed_at: datetime | None, *, tolerance_minutes: int) -> bool:
    observed_utc = ensure_utc(observed_at)
    if observed_utc is None:
        return False
    return observed_utc > utc_now() + timedelta(minutes=tolerance_minutes)


def _append_dashboard_audit(
    session,
    *,
    guid: str | None,
    company_slug: str | None,
    device_id: str | None,
    fleet_id: str | None,
    plate_no: str | None,
    observed_at: datetime | None,
    received_at: datetime,
    raw_alarm_type: str | None,
    raw_tp: str | None,
    raw_event_code: str | None,
    stage: str,
    reason: str,
    payload: dict[str, Any],
) -> None:
    if guid:
        existing = session.scalar(
            select(AlarmEventAudit.id).where(
                AlarmEventAudit.guid == guid,
                AlarmEventAudit.stage == stage,
                AlarmEventAudit.reason == reason,
            )
        )
        if existing:
            return
    session.add(
        AlarmEventAudit(
            guid=guid,
            company_slug=company_slug,
            device_id=device_id,
            fleet_id=fleet_id,
            plate_no=plate_no,
            observed_at=ensure_utc(observed_at),
            received_at=ensure_utc(received_at) or utc_now(),
            raw_alarm_type=raw_alarm_type,
            raw_tp=raw_tp,
            raw_event_code=raw_event_code,
            stage=stage,
            reason=reason,
            payload_json=json.dumps(payload, ensure_ascii=True),
        )
    )


def _max_or_value(current: datetime | None, candidate: datetime | None) -> datetime | None:
    current = ensure_utc(current)
    candidate = ensure_utc(candidate)
    if current is None:
        return candidate
    if candidate is None:
        return current
    return max(current, candidate)


def _next_cut_boundary(base_at: datetime, interval_minutes: int) -> datetime:
    base_at = ensure_utc(base_at) or utc_now()
    safe_interval = max(interval_minutes, 1)
    interval_seconds = safe_interval * 60
    epoch_seconds = int(base_at.timestamp())
    aligned_seconds = (epoch_seconds // interval_seconds) * interval_seconds
    aligned = datetime.fromtimestamp(aligned_seconds, tz=ZoneInfo("UTC"))
    if aligned <= base_at:
        aligned += timedelta(minutes=safe_interval)
    return aligned


def _current_harvest_cut(*, interval_minutes: int, lag_seconds: int) -> datetime:
    now_utc = utc_now() - timedelta(seconds=max(lag_seconds, 0))
    safe_interval = max(interval_minutes, 1)
    interval_seconds = safe_interval * 60
    aligned_seconds = int(now_utc.timestamp()) // interval_seconds * interval_seconds
    return datetime.fromtimestamp(aligned_seconds, tz=ZoneInfo("UTC"))
