from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
import json
from statistics import mean
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import or_, select

from app.core.catalog import CATEGORY_META, CATEGORY_ORDER
from app.core.time import as_timezone, ensure_utc, utc_now
from app.models import AlarmEvent, AlarmEventAudit, DailyMileageSnapshot, DeviceRecord, IngestState, IngestionAnomaly, MileageReading, ReportAsset
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
    ReconciliationDrilldownRow,
    ReconciliationRunRequest,
    ReconciliationSummary,
    RecentAuditView,
    ReportFileView,
    ReportsSummaryView,
    UnclassifiedCodeView,
)
from app.services.company_registry import CompanyRegistry
from app.services.howen import HowenClient

ACTIVE_EVENT_SOURCES = ("live", "backfill")
ACTIVE_SNAPSHOT_SOURCES = ("live", "backfill")
ACTIVE_MILEAGE_SOURCES = ("status", "live", "backfill")
MOCK_DEVICE_PREFIX = "DEV-"
MOCK_FLEET_IDS = {"cotaba-main"}


class DashboardService:
    def __init__(self, *, session_factory: Any, registry: CompanyRegistry, settings: Any) -> None:
        self.session_factory = session_factory
        self.registry = registry
        self.settings = settings
        self.howen = HowenClient(settings=settings, registry=registry)

    def build_snapshot(self, company_slug: str) -> dict[str, Any]:
        company = self.registry.get(company_slug)
        tz = ZoneInfo(company.timezone or self.settings.default_timezone)
        now_utc = utc_now()
        now_local = now_utc.astimezone(tz)
        cutoff = now_utc - timedelta(days=self.settings.live_retention_days)
        recent_cutoff = now_utc - timedelta(hours=24)
        anomaly_cutoff = now_utc - timedelta(hours=24)

        with self.session_factory() as session:
            events = list(
                session.scalars(
                    select(AlarmEvent)
                    .where(AlarmEvent.source.in_(ACTIVE_EVENT_SOURCES), AlarmEvent.occurred_at >= cutoff)
                    .order_by(AlarmEvent.occurred_at)
                )
            )
            daily_snapshots = list(
                session.scalars(
                    select(DailyMileageSnapshot)
                    .where(DailyMileageSnapshot.source.in_(ACTIVE_SNAPSHOT_SOURCES), DailyMileageSnapshot.observed_at >= cutoff)
                    .order_by(DailyMileageSnapshot.observed_at)
                )
            )
            legacy_mileages = list(
                session.scalars(
                    select(MileageReading)
                    .where(MileageReading.source.in_(ACTIVE_MILEAGE_SOURCES), MileageReading.recorded_at >= cutoff)
                    .order_by(MileageReading.recorded_at)
                )
            )
            devices = list(
                session.scalars(
                    select(DeviceRecord)
                    .where(DeviceRecord.record_source == "live")
                    .order_by(DeviceRecord.device_id)
                )
            )
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
                    .where(IngestionAnomaly.received_at >= anomaly_cutoff, IngestionAnomaly.company_slug == company_slug)
                    .order_by(IngestionAnomaly.received_at.desc())
                )
            )
            state = session.get(IngestState, "global")

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
            state.last_device_sync_at = ensure_utc(state.last_device_sync_at)
            state.last_anomaly_at = ensure_utc(state.last_anomaly_at)

        company_events = [
            event
            for event in events
            if self.registry.device_belongs(company, event.device_id, event.fleet_id)
            and self.registry.category_allowed(company, event.category)
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

        dates_30 = _date_window(now_local.date(), 30)
        dates_7 = dates_30[-7:]
        dates_30_set = set(dates_30)
        dates_7_set = set(dates_7)
        latest_day = dates_30[-1]
        closed_days = dates_30[:-1]
        daily_km_by_vehicle, fleet_km_by_date = _build_daily_km(company_snapshots, company_legacy, company_events, tz)
        _merge_current_day_from_device_state(company_devices, daily_km_by_vehicle, fleet_km_by_date, latest_day, tz)
        recent_events = [_serialize_event(event, tz) for event in company_events if event.occurred_at >= recent_cutoff]
        current_day_km_provisional = round(fleet_km_by_date.get(latest_day, 0.0), 1)
        km_total_closed_window = round(sum(fleet_km_by_date.get(day_key, 0.0) for day_key in closed_days), 1)

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
                    "por100km": _rate(total_30, km_30),
                    "riesgo100km": _rate(risk_score, km_30),
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

        return {
            "meta": {
                "companySlug": company.slug,
                "companyName": company.name,
                "customer": company.customer,
                "brand": company.brand.model_dump(),
                "generatedAt": now_utc.isoformat(),
                "timezone": company.timezone,
                "rangeStart": dates_30[0].isoformat(),
                "rangeEnd": latest_day.isoformat(),
                "vehicleCount": len({device.device_id for device in company_devices}),
                "ingestMode": "live",
                "kmTotal": total_km_30,
                "kmTotalClosedWindow": km_total_closed_window,
                "currentDayKmProvisional": current_day_km_provisional,
                "currentDayIsProvisional": True,
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
                    "por100km": _rate(total_30, total_km_30),
                    "nocturno_pct": round((nocturnal_30 / total_30) * 100, 1) if total_30 else 0,
                    "rango": f"{dates_30[0].isoformat()} a {latest_day.isoformat()}",
                },
                "semana": semana,
                "fechas": [day_key.isoformat() for day_key in dates_30],
                "serie_cat": serie_cat,
                "km_dia": [round(fleet_km_by_date.get(day_key, 0.0), 1) for day_key in dates_30],
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
        with self.session_factory() as session:
            state = session.get(IngestState, "global")
            anomaly_query = session.query(IngestionAnomaly).filter(IngestionAnomaly.received_at >= anomaly_cutoff)
            if company_slug:
                anomaly_query = anomaly_query.filter(IngestionAnomaly.company_slug == company_slug)
            anomaly_count = anomaly_query.count()
        if state:
            state.last_cycle_received_at = ensure_utc(state.last_cycle_received_at)
            state.last_event_observed_at = ensure_utc(state.last_event_observed_at)
            state.last_status_at = ensure_utc(state.last_status_at)
            state.last_alarm_at = ensure_utc(state.last_alarm_at)
            state.last_device_sync_at = ensure_utc(state.last_device_sync_at)
        return AdminIngestionStatusView(
            mode=state.mode if state else "live",
            connection_state=state.connection_state if state else "idle",
            last_cycle_received_at=state.last_cycle_received_at if state else None,
            last_event_observed_at=state.last_event_observed_at if state else None,
            last_alarm_at=state.last_alarm_at if state else None,
            last_status_at=state.last_status_at if state else None,
            last_device_sync_at=state.last_device_sync_at if state else None,
            last_error=state.last_error if state else None,
            anomaly_count_24h=anomaly_count,
        ).model_dump(mode="json")

    def build_admin_overview(self, company_slug: str) -> dict[str, Any]:
        company = self.registry.get(company_slug)
        snapshot = self.build_snapshot(company_slug)
        now_utc = utc_now()
        with self.session_factory() as session:
            devices = [
                device
                for device in session.scalars(select(DeviceRecord).where(DeviceRecord.record_source == "live").order_by(DeviceRecord.plate_no))
                if self.registry.device_belongs(company, device.device_id, device.fleet_id)
            ]
            reports = list(
                session.scalars(
                    select(ReportAsset)
                    .where(ReportAsset.company_slug == company_slug)
                    .order_by(ReportAsset.year.desc(), ReportAsset.month.desc())
                )
            )
            last_day = date.fromisoformat(snapshot["meta"]["rangeEnd"])
            snapshots_today = list(
                session.scalars(
                    select(DailyMileageSnapshot).where(
                        DailyMileageSnapshot.source.in_(ACTIVE_SNAPSHOT_SOURCES),
                        DailyMileageSnapshot.snapshot_date == last_day,
                    )
                )
            )
            alarms_24h = list(
                session.scalars(
                    select(AlarmEvent).where(
                        AlarmEvent.source.in_(ACTIVE_EVENT_SOURCES),
                        AlarmEvent.occurred_at >= now_utc - timedelta(hours=24),
                    )
                )
            )

        stale_vehicles = sum(
            1
            for device in devices
            if not device.last_received_at or (now_utc - ensure_utc(device.last_received_at)).total_seconds() / 60 >= company.rules.feed_stopped_threshold_minutes
        )
        reporting_vehicles_24h = sum(
            1
            for device in devices
            if device.last_received_at and ensure_utc(device.last_received_at) >= now_utc - timedelta(hours=24)
        )
        vehicles_with_snapshot_today = len(
            {
                snapshot_row.device_id
                for snapshot_row in snapshots_today
                if self.registry.device_belongs(company, snapshot_row.device_id, snapshot_row.fleet_id)
            }
        )
        vehicles_with_alarm_24h = len(
            {
                alarm.device_id
                for alarm in alarms_24h
                if self.registry.device_belongs(company, alarm.device_id, alarm.fleet_id)
                and self.registry.category_allowed(company, alarm.category)
            }
        )
        latest_report = reports[0] if reports else None

        return AdminOverviewView(
            company_slug=company.slug,
            company_name=company.name,
            ingest_mode="live",
            feed=FeedState.model_validate(snapshot["feed"]),
            coverage=CoverageSummaryView(
                total_vehicles=len(devices),
                reporting_vehicles_24h=reporting_vehicles_24h,
                stale_vehicles=stale_vehicles,
                vehicles_with_snapshot_today=vehicles_with_snapshot_today,
                vehicles_with_alarm_24h=vehicles_with_alarm_24h,
            ),
            km=KmSummaryView(
                total_window_km=snapshot["meta"]["kmTotal"],
                closed_window_km=snapshot["meta"]["kmTotalClosedWindow"],
                current_day_km_provisional=snapshot["meta"]["currentDayKmProvisional"],
                current_day_label=last_day,
            ),
            reports=ReportsSummaryView(
                available_reports=len(snapshot["reports"]),
                latest_report_year=latest_report.year if latest_report else None,
                latest_report_month=latest_report.month if latest_report else None,
            ),
            anomaly_count_24h=snapshot["dataQuality"]["anomaly_count_24h"],
            active_notes=snapshot["dataQuality"]["active_notes"],
        ).model_dump(mode="json")

    def build_admin_live_setup(self, company_slug: str) -> dict[str, Any]:
        company = self.registry.get(company_slug)
        now_utc = utc_now()
        recent_alarm_cutoff = now_utc - timedelta(days=7)
        recent_status_cutoff = now_utc - timedelta(hours=24)

        with self.session_factory() as session:
            devices = list(session.scalars(select(DeviceRecord).order_by(DeviceRecord.fleet_id, DeviceRecord.plate_no, DeviceRecord.device_id)))
            recent_alarms = list(
                session.scalars(
                    select(AlarmEvent).where(AlarmEvent.occurred_at >= recent_alarm_cutoff).order_by(AlarmEvent.occurred_at.desc())
                )
            )
            all_alarms = list(session.scalars(select(AlarmEvent)))
            all_snapshots = list(session.scalars(select(DailyMileageSnapshot)))

        for device in devices:
            device.last_received_at = ensure_utc(device.last_received_at)
            device.last_seen_at = ensure_utc(device.last_seen_at)
        for alarm in recent_alarms:
            alarm.occurred_at = ensure_utc(alarm.occurred_at) or alarm.occurred_at
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
        for alarm in recent_alarms:
            if _is_mock_identity(alarm.device_id, alarm.fleet_id) or alarm.category != "Sin clasificar":
                continue
            key = (alarm.subtype, alarm.event_code)
            row = unclassified_groups.setdefault(
                key,
                {
                    "subtype": alarm.subtype,
                    "event_code": alarm.event_code,
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

        return AdminLiveSetupView(
            company_slug=company.slug,
            company_name=company.name,
            assignment=CompanyAssignmentView.model_validate(assignment),
            mock_data=MockDataSummaryView.model_validate(mock_data),
            fleet_candidates=[FleetCandidateView.model_validate(item) for item in fleet_candidates],
            unclassified_codes=[UnclassifiedCodeView.model_validate(item) for item in unclassified_codes],
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
        company = self.registry.get(company_slug)
        tz = ZoneInfo(company.timezone or self.settings.default_timezone)
        recent_24h_start = max(start_at, utc_now() - timedelta(hours=24))
        baseline_start = recent_24h_start.astimezone(tz).date() - timedelta(days=30)
        with self.session_factory() as session:
            all_company_alarms = [
                event
                for event in session.scalars(
                    select(AlarmEvent)
                    .where(AlarmEvent.source.in_(ACTIVE_EVENT_SOURCES), AlarmEvent.occurred_at >= start_at, AlarmEvent.occurred_at <= end_at)
                    .order_by(AlarmEvent.occurred_at)
                )
                if self.registry.device_belongs(company, event.device_id, event.fleet_id)
            ]
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
                    select(DailyMileageSnapshot)
                    .where(
                        DailyMileageSnapshot.source.in_(ACTIVE_SNAPSHOT_SOURCES),
                        DailyMileageSnapshot.snapshot_date >= baseline_start,
                    )
                    .order_by(DailyMileageSnapshot.snapshot_date, DailyMileageSnapshot.observed_at)
                )
                if self.registry.device_belongs(company, snapshot.device_id, snapshot.fleet_id)
            ]

        for event in all_company_alarms:
            event.occurred_at = ensure_utc(event.occurred_at) or event.occurred_at
        for snapshot in baseline_snapshots:
            snapshot.observed_at = ensure_utc(snapshot.observed_at) or snapshot.observed_at
        for audit_row in alarm_audits:
            audit_row.received_at = ensure_utc(audit_row.received_at) or audit_row.received_at
            audit_row.observed_at = ensure_utc(audit_row.observed_at)
        visible_alarms = [event for event in all_company_alarms if event.classification_status == "classified_dms"]
        recent_visible_alarms = [event for event in visible_alarms if event.occurred_at >= recent_24h_start]
        daily_km_by_vehicle, _ = _build_daily_km(baseline_snapshots, [], all_company_alarms, tz)
        recent_analysis = _build_recent_episode_analysis(recent_visible_alarms, company, tz, daily_km_by_vehicle)
        recent_metrics = dict(recent_analysis["metrics"])
        recent_metrics["non_dms_hidden"] = sum(
            1
            for event in all_company_alarms
            if event.occurred_at >= recent_24h_start and event.classification_status == "classified_non_dms"
        )
        recent_metrics["unmapped_hidden"] = sum(
            1
            for event in all_company_alarms
            if event.occurred_at >= recent_24h_start and event.classification_status == "unmapped"
        )
        recent_metrics["future_rejected"] = sum(
            1
            for audit_row in alarm_audits
            if audit_row.received_at >= recent_24h_start and audit_row.reason == "future_timestamp"
        )

        return AdminAuditView(
            company_slug=company.slug,
            company_name=company.name,
            range_start=start_at,
            range_end=end_at,
            alarms=AlarmAuditView(
                accepted_total=len(all_company_alarms),
                visible_total=len(visible_alarms),
                unclassified_total=sum(1 for event in all_company_alarms if event.classification_status == "unmapped"),
                mapping_sources=dict(Counter(event.mapping_source or "unknown" for event in all_company_alarms)),
                by_category=dict(Counter(event.category for event in all_company_alarms)),
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
            recent_24h=RecentAuditView(**recent_metrics),
        ).model_dump(mode="json")

    async def run_reconciliation(self, payload: ReconciliationRunRequest) -> dict[str, Any]:
        company = self.registry.get(payload.company_slug)
        range_start, range_end = _resolve_reconciliation_range(
            company=company,
            start_at=payload.from_at,
            end_at=payload.to_at,
            window_type=payload.window_type,
            fallback_timezone=self.settings.default_timezone,
        )
        portal_rows = await self._fetch_portal_rows(company, range_start=range_start, range_end=range_end)
        self._sync_portal_rows(company=company, portal_rows=portal_rows)
        summary, _ = self._build_reconciliation_report(
            company=company,
            range_start=range_start,
            range_end=range_end,
            window_type=payload.window_type,
            portal_rows=portal_rows,
        )
        return summary

    async def build_reconciliation_summary(
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
        portal_rows = await self._fetch_portal_rows(company, range_start=range_start, range_end=range_end)
        summary, _ = self._build_reconciliation_report(
            company=company,
            range_start=range_start,
            range_end=range_end,
            window_type=window_type,
            portal_rows=portal_rows,
        )
        return summary

    async def build_reconciliation_drilldown(
        self,
        *,
        company_slug: str,
        start_at: datetime,
        end_at: datetime,
        window_type: str,
    ) -> list[dict[str, Any]]:
        company = self.registry.get(company_slug)
        range_start, range_end = _resolve_reconciliation_range(
            company=company,
            start_at=start_at,
            end_at=end_at,
            window_type=window_type,
            fallback_timezone=self.settings.default_timezone,
        )
        portal_rows = await self._fetch_portal_rows(company, range_start=range_start, range_end=range_end)
        _, rows = self._build_reconciliation_report(
            company=company,
            range_start=range_start,
            range_end=range_end,
            window_type=window_type,
            portal_rows=portal_rows,
        )
        return rows

    def build_km_quality(self, company_slug: str) -> dict[str, Any]:
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
        samples: list[str] = []
        for device in devices:
            snapshot = latest_snapshot_by_device.get(device.device_id)
            if device.km_validation_reason and "total_regression" in device.km_validation_reason:
                total_regression += 1
            total_reference = device.last_total_km
            device_valid = _is_valid_day_km(device.last_day_km, total_reference)
            snapshot_valid = bool(snapshot and _is_valid_day_km(snapshot.day_km, snapshot.total_km))
            is_valid = device_valid or snapshot_valid
            if is_valid:
                valid_day += 1
            else:
                invalid_day += 1
                if device.plate_no and len(samples) < 8:
                    samples.append(device.plate_no)
            if snapshot and snapshot.km_validation_reason and "total_regression" in snapshot.km_validation_reason:
                total_regression += 1

        repaired_rows = sum(1 for snapshot in snapshots if snapshot.repaired_at is not None)
        return KmQualitySummary(
            company_slug=company.slug,
            company_name=company.name,
            vehicles_with_valid_day_km=valid_day,
            vehicles_with_invalid_day_km=invalid_day,
            vehicles_with_total_regression=total_regression,
            current_day_km_source="device_state_or_alarm_derived",
            repaired_rows=repaired_rows,
            sample_invalid_vehicles=samples,
        ).model_dump(mode="json")

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
        session = await self.howen.resolve_session(force_login=False)
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
        for device_id in device_ids:
            try:
                portal_rows.extend(
                    await self.howen.fetch_historical_alarms(
                        session.token,
                        device_id=device_id,
                        start_at=start_local,
                        end_at=end_local,
                    )
                )
            except Exception as exc:
                if not self.howen.is_auth_error(exc):
                    raise
                self.howen.clear_cached_session()
                session = await self.howen.resolve_session(force_login=True)
                portal_rows.extend(
                    await self.howen.fetch_historical_alarms(
                        session.token,
                        device_id=device_id,
                        start_at=start_local,
                        end_at=end_local,
                    )
                )
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

        for event in local_alarms:
            event.occurred_at = ensure_utc(event.occurred_at) or event.occurred_at
        for snapshot in baseline_snapshots:
            snapshot.observed_at = ensure_utc(snapshot.observed_at) or snapshot.observed_at

        local_by_guid = {event.guid: event for event in local_alarms}
        dms_local_alarms = [event for event in local_alarms if event.classification_status == "classified_dms"]
        daily_km_by_vehicle, _ = _build_daily_km(baseline_snapshots, [], local_alarms, tz)
        episode_analysis = _build_recent_episode_analysis(dms_local_alarms, company, tz, daily_km_by_vehicle)
        guid_status = episode_analysis["guid_status"]

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

        for portal_row in portal_rows:
            normalized = self.howen.normalize_alarm(portal_row)
            guid = normalized.guid if normalized else (_payload_guid(portal_row) or f"raw-{raw_portal_equivalent + 1}")
            if guid in seen_guids:
                continue
            seen_guids.add(guid)
            raw_portal_equivalent += 1

            if not normalized:
                unmapped += 1
                rows.append(
                    ReconciliationDrilldownRow(
                        guid=guid,
                        plate_no=_payload_plate(portal_row),
                        device_id=_payload_device(portal_row),
                        raw_alarm_type=_payload_alarm_type(portal_row),
                        raw_tp=_payload_alarm_tp(portal_row),
                        raw_event_code=_payload_alarm_event_code(portal_row),
                        observed_at=None,
                        classification_status="unmapped",
                        visibility_status="hidden_unmapped",
                        source="portal_raw",
                        category=None,
                        subtype=None,
                        reason="normalization_failed",
                    ).model_dump(mode="json")
                )
                continue

            future_rejected = _is_future_event(
                normalized.occurred_at,
                tolerance_minutes=self.settings.anomaly_future_tolerance_minutes,
            )
            local_match = local_by_guid.get(normalized.guid)
            if local_match and local_match.source == "live":
                ingested_live += 1
            elif local_match and local_match.source == "backfill":
                ingested_backfill += 1

            if normalized.classification_status == "classified_dms":
                classified_dms += 1
            elif normalized.classification_status == "classified_non_dms":
                classified_non_dms += 1
            else:
                unmapped += 1

            reason = "classified_non_dms"
            visibility_status = normalized.visibility_status
            episode_guid = None
            episode_title = None
            source_label = local_match.source if local_match else "portal_raw"
            category = local_match.category if local_match else normalized.category
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
            elif not local_match:
                missing_local += 1
                reason = "missing_local"
                visibility_status = "missing_local"
            elif local_match.classification_status != "classified_dms":
                missing_local += 1
                reason = f"stored_local_{local_match.classification_status or 'unknown'}"
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

            rows.append(
                ReconciliationDrilldownRow(
                    guid=normalized.guid,
                    plate_no=local_match.plate_no if local_match else normalized.plate_no,
                    device_id=normalized.device_id,
                    raw_alarm_type=normalized.raw_alarm_type,
                    raw_tp=normalized.raw_tp,
                    raw_event_code=normalized.raw_event_code,
                    observed_at=normalized.occurred_at,
                    classification_status=normalized.classification_status,
                    visibility_status=visibility_status,
                    source=source_label,
                    category=category,
                    subtype=subtype,
                    reason=reason,
                    episode_guid=episode_guid,
                    episode_title=episode_title,
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

    def _sync_portal_rows(self, *, company: CompanyConfig, portal_rows: list[dict[str, Any]]) -> None:
        now = utc_now()
        with self.session_factory() as session:
            snapshot_cache: dict[tuple[str, date], DailyMileageSnapshot | None] = {}
            for portal_row in portal_rows:
                normalized = self.howen.normalize_alarm(portal_row)
                if not normalized or _is_future_event(
                    normalized.occurred_at,
                    tolerance_minutes=self.settings.anomaly_future_tolerance_minutes,
                ):
                    continue
                occurred_at = ensure_utc(normalized.occurred_at) or normalized.occurred_at
                existing = session.get(AlarmEvent, normalized.guid)
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

                audit_reason = None
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
                    select(AlarmEvent)
                    .where(AlarmEvent.source.in_(ACTIVE_EVENT_SOURCES), AlarmEvent.occurred_at >= utc_now() - timedelta(days=7))
                    .order_by(AlarmEvent.occurred_at.desc())
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
                last_alarm_by_device[alarm.device_id] = ensure_utc(alarm.occurred_at) or alarm.occurred_at

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

    def list_anomalies(self, *, company_slug: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        with self.session_factory() as session:
            query = select(IngestionAnomaly).order_by(IngestionAnomaly.received_at.desc()).limit(limit)
            if company_slug:
                query = query.where(IngestionAnomaly.company_slug == company_slug)
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
            connection_state=state.connection_state,
            last_error=state.last_error,
        ).model_dump(mode="json")


def _build_daily_km(
    daily_snapshots: list[DailyMileageSnapshot],
    legacy_mileages: list[MileageReading],
    alarm_events: list[AlarmEvent],
    tz: ZoneInfo,
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

    alarm_samples: dict[str, dict[date, list[tuple[datetime, float]]]] = defaultdict(lambda: defaultdict(list))
    for event in alarm_events:
        if event.total_mileage_km is None:
            continue
        normalized_total = _normalize_persisted_alarm_km(event.total_mileage_km)
        if normalized_total is None:
            continue
        plate = event.plate_no or event.device_id
        day_key = ensure_utc(event.occurred_at).astimezone(tz).date()
        alarm_samples[plate][day_key].append((event.occurred_at, normalized_total))

    for plate, days in alarm_samples.items():
        previous_end_total: float | None = None
        for day_key in sorted(days):
            samples = sorted(days[day_key], key=lambda item: item[0])
            values = [value for _, value in samples]
            if not values:
                continue
            day_min = min(values)
            day_max = max(values)
            km = max(day_max - day_min, 0.0)
            if km <= 0 and previous_end_total is not None and day_max >= previous_end_total:
                km = max(day_max - previous_end_total, 0.0)
            _merge_daily_km_value(grouped, fleet_by_date, plate, day_key, km)
            previous_end_total = day_max
    return grouped, fleet_by_date


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
) -> dict[str, int]:
    return dict(_build_recent_episode_analysis(events, company, tz, daily_km_by_vehicle)["metrics"])


def _build_recent_episode_analysis(
    events: list[AlarmEvent],
    company: CompanyConfig,
    tz: ZoneInfo,
    daily_km_by_vehicle: dict[str, dict[date, float]],
) -> dict[str, Any]:
    if not events:
        return {
            "metrics": {
                "raw_events": 0,
                "grouped_episodes": 0,
                "visible_alerts": 0,
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

    raw_events = sorted(events, key=lambda event: event.occurred_at.astimezone(tz))
    grouped: list[dict[str, Any]] = []
    open_groups: dict[str, dict[str, Any]] = {}
    for event in raw_events:
        key = f"{event.plate_no or event.device_id}|{event.category}"
        current = open_groups.get(key)
        event_dt = event.occurred_at.astimezone(tz)
        gap_seconds = (
            (event_dt - current["events"][-1].occurred_at.astimezone(tz)).total_seconds() if current else None
        )
        gap_minutes = gap_seconds / 60 if gap_seconds is not None else None
        window_minutes = (
            company.rules.collision_window_minutes
            if event.category == "Riesgo de colision"
            else company.rules.yawn_window_minutes
            if event.category == "Bostezo"
            else company.rules.streak_window_minutes
        )

        if current and gap_minutes is not None and gap_minutes <= window_minutes:
            if gap_seconds is not None and gap_seconds <= company.rules.echo_window_seconds:
                current["events"][-1] = event
            else:
                current["events"].append(event)
            continue

        next_group = {
            "plate": event.plate_no or event.device_id,
            "category": event.category,
            "events": [event],
            "group_id": (event.plate_no or event.device_id, event.category, event.guid),
        }
        open_groups[key] = next_group
        grouped.append(next_group)

    visible = 0
    dismissed = 0
    suppressed_raw = 0
    visible_raw = 0
    consumed: set[tuple[str, str, str]] = set()
    guid_status: dict[str, dict[str, Any]] = {}
    episodes: list[dict[str, Any]] = []
    company_events_by_vehicle_day = defaultdict(Counter)
    for event in raw_events:
        plate = event.plate_no or event.device_id
        day_key = event.occurred_at.astimezone(tz).date()
        company_events_by_vehicle_day[plate][day_key] += 1
    baselines = _build_baselines(
        baseline_dates=_date_window(utc_now().astimezone(tz).date() - timedelta(days=1), 30),
        daily_km_by_vehicle=daily_km_by_vehicle,
        events_by_vehicle_day=company_events_by_vehicle_day,
    )
    today = utc_now().astimezone(tz).date()
    today_counts = Counter(
        (event.plate_no or event.device_id) for event in raw_events if event.occurred_at.astimezone(tz).date() == today
    )

    for group in grouped:
        first = group["events"][0]
        last = group["events"][-1]
        plate = group["plate"]
        group_key = group["group_id"]
        if group_key in consumed:
            continue

        same_day_count = sum(
            1
            for event in raw_events
            if (event.plate_no or event.device_id) == plate
            and event.category == group["category"]
            and event.occurred_at.astimezone(tz).date() == last.occurred_at.astimezone(tz).date()
        )
        baseline = baselines.get(plate, 0.0)
        deviation = round((today_counts.get(plate, 0) / baseline), 2) if baseline else float(today_counts.get(plate, 0) or 1.0)
        current_category = group["category"]
        reason = "visible"
        merged_groups = [group]
        episode_title = current_category

        if group["category"] == "Ojos cerrados":
            matching_yawn = next(
                (
                    candidate
                    for candidate in grouped
                    if candidate["plate"] == plate
                    and candidate["category"] == "Bostezo"
                    and candidate["events"][-1].occurred_at.astimezone(tz) < first.occurred_at.astimezone(tz)
                    and (
                        first.occurred_at.astimezone(tz) - candidate["events"][-1].occurred_at.astimezone(tz)
                    ).total_seconds()
                    / 60
                    <= company.rules.fatigue_merge_window_minutes
                ),
                None,
            )
            if matching_yawn:
                consumed.add(matching_yawn["group_id"])
                merged_groups.append(matching_yawn)
                current_category = "Fatiga en progresion"
                reason = "merged_yawn_into_fatigue"
                episode_title = "Fatiga en progresion"
            elif len(group["events"]) >= company.rules.eyes_closed_critical_threshold or len(group["events"]) == 2:
                episode_title = "Ojos cerrados"
            else:
                dismissed += 1
                suppressed_raw += len(group["events"])
                for event in group["events"]:
                    guid_status[event.guid] = {
                        "visibility_status": "suppressed_by_rule",
                        "reason": "single_eye_closed",
                        "episode_guid": None,
                        "episode_title": None,
                    }
                continue
        elif group["category"] == "Distraccion":
            if deviation < 3:
                dismissed += 1
                suppressed_raw += len(group["events"])
                for event in group["events"]:
                    guid_status[event.guid] = {
                        "visibility_status": "suppressed_by_rule",
                        "reason": "distraction_below_3x",
                        "episode_guid": None,
                        "episode_title": None,
                    }
                continue
            episode_title = "Distraccion"
        elif group["category"] == "Uso de celular":
            episode_title = "Uso de celular"
        elif group["category"] == "Riesgo de colision":
            episode_title = "Riesgo de colision"
        elif group["category"] == "Bostezo":
            episode_title = "Bostezo"
        elif group["category"] == "Camara cubierta":
            episode_title = "Camara cubierta"

        if same_day_count > company.rules.anti_noise_daily_cap:
            reason = "anti_noise_daily_cap"
            episode_title = f"{group['category']} fuera de patron"

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
            }

    return {
        "metrics": {
            "raw_events": len(raw_events),
            "grouped_episodes": len(grouped),
            "visible_alerts": visible,
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


def _serialize_event(event: AlarmEvent, tz: ZoneInfo) -> dict[str, Any]:
    return {
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


def _payload_guid(payload: dict[str, Any]) -> str | None:
    return _string_or_none(_nested_value(payload, "alarmID") or _nested_value(payload, "guid") or _nested_value(payload, "uuid"))


def _payload_plate(payload: dict[str, Any]) -> str | None:
    return _string_or_none(_nested_value(payload, "plateNo") or _nested_value(payload, "plateno") or _nested_value(payload, "plate"))


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
