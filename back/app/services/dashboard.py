from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from statistics import mean
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import or_, select

from app.core.catalog import CATEGORY_META, CATEGORY_ORDER
from app.core.time import as_timezone, ensure_utc, utc_now
from app.models import AlarmEvent, DailyMileageSnapshot, DeviceRecord, IngestState, IngestionAnomaly, MileageReading, ReportAsset
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
    KmSummaryView,
    MockDataPurgeResult,
    MockDataSummaryView,
    RecentAuditView,
    ReportFileView,
    ReportsSummaryView,
    UnclassifiedCodeView,
)
from app.services.company_registry import CompanyRegistry

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
        weekly_line = [row["placa"] for row in table_rows[:5]]
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
                for plate in weekly_line
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
        visible_alarms = [event for event in all_company_alarms if self.registry.category_allowed(company, event.category)]
        recent_visible_alarms = [event for event in visible_alarms if event.occurred_at >= recent_24h_start]
        daily_km_by_vehicle, _ = _build_daily_km(baseline_snapshots, [], all_company_alarms, tz)
        recent_metrics = _build_recent_episode_metrics(recent_visible_alarms, company, tz, daily_km_by_vehicle)

        return AdminAuditView(
            company_slug=company.slug,
            company_name=company.name,
            range_start=start_at,
            range_end=end_at,
            alarms=AlarmAuditView(
                accepted_total=len(all_company_alarms),
                visible_total=len(visible_alarms),
                unclassified_total=sum(1 for event in visible_alarms if event.category == "Sin clasificar"),
                mapping_sources=dict(Counter(event.mapping_source or "unknown" for event in visible_alarms)),
                by_category=dict(Counter(event.category for event in visible_alarms)),
                by_subtype=[
                    {"subtype": subtype or "sin_subtipo", "count": count}
                    for subtype, count in Counter(event.subtype or "" for event in visible_alarms).most_common(20)
                ],
            ),
            anomalies=AnomalyAuditView(
                total=len(anomalies),
                by_reason=dict(Counter(anomaly.reason for anomaly in anomalies)),
            ),
            recent_24h=RecentAuditView(**recent_metrics),
        ).model_dump(mode="json")

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
            if snapshot.day_km is not None:
                km = round(max(snapshot.day_km, 0.0), 1)
            elif previous_total is not None:
                km = round(max((snapshot.total_km or 0.0) - previous_total, 0.0), 1)
            else:
                km = round(max(snapshot.total_km or 0.0, 0.0), 1)
            _merge_daily_km_value(grouped, fleet_by_date, plate, day_key, km, replace=True)
            previous_total = snapshot.total_km

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
        if device.last_day_km is None:
            continue
        plate = device.plate_no or device.device_id
        next_value = round(max(device.last_day_km, 0.0), 1)
        current_value = daily_km_by_vehicle[plate].get(latest_day, 0.0)
        if next_value <= current_value:
            continue
        daily_km_by_vehicle[plate][latest_day] = next_value
        fleet_km_by_date[latest_day] += next_value - current_value


def _build_recent_episode_metrics(
    events: list[AlarmEvent],
    company: CompanyConfig,
    tz: ZoneInfo,
    daily_km_by_vehicle: dict[str, dict[date, float]],
) -> dict[str, int]:
    if not events:
        return {
            "raw_events": 0,
            "grouped_episodes": 0,
            "visible_alerts": 0,
            "dismissed_alerts": 0,
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

        next_group = {"plate": event.plate_no or event.device_id, "category": event.category, "events": [event]}
        open_groups[key] = next_group
        grouped.append(next_group)

    visible = 0
    dismissed = 0
    consumed: set[tuple[str, str, str]] = set()
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
        group_key = (plate, group["category"], first.guid)
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
                consumed.add((plate, "Bostezo", matching_yawn["events"][0].guid))
                visible += 1
            elif len(group["events"]) >= company.rules.eyes_closed_critical_threshold or len(group["events"]) == 2:
                visible += 1
            else:
                dismissed += 1
        elif group["category"] == "Distraccion":
            if deviation < 3:
                dismissed += 1
            else:
                visible += 1
        else:
            visible += 1

    return {
        "raw_events": len(raw_events),
        "grouped_episodes": len(grouped),
        "visible_alerts": visible,
        "dismissed_alerts": dismissed,
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
