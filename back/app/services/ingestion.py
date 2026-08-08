from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import delete, select

from app.core.time import ensure_utc, to_local_date, utc_now
from app.models import AlarmEvent, AlarmEventAudit, DailyMileageSnapshot, DeviceRecord, IngestState, IngestionAnomaly, MileageReading
from app.schemas import BackfillRequest, NormalizedAlarm, NormalizedStatus
from app.services.company_registry import CompanyRegistry
from app.services.dashboard import DashboardService
from app.services.howen import HowenClient
from app.services.realtime_hub import RealtimeHub


class IngestionService:
    def __init__(
        self,
        *,
        settings: Any,
        session_factory: Any,
        registry: CompanyRegistry,
        dashboard: DashboardService,
        hub: RealtimeHub,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.registry = registry
        self.dashboard = dashboard
        self.hub = hub
        self.howen = HowenClient(settings=settings, registry=registry)
        self._runner_task: asyncio.Task[None] | None = None
        self._publisher_task: asyncio.Task[None] | None = None
        self._dirty = asyncio.Event()
        self._last_purge_at = None
        self._last_device_sync_at = None
        self._last_operational_catchup_at: dict[str, Any] = {}

    async def start(self) -> None:
        self._ensure_state_row()
        self._runner_task = asyncio.create_task(self._run_live_forever(), name="ingestion-runner")
        self._publisher_task = asyncio.create_task(self._publisher_loop(), name="dashboard-publisher")
        self.mark_dirty()

    async def stop(self) -> None:
        tasks = [task for task in (self._runner_task, self._publisher_task) if task]
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        try:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=2.0)
        except asyncio.TimeoutError:
            pass
        finally:
            self._runner_task = None
            self._publisher_task = None

    def mark_dirty(self) -> None:
        self._dirty.set()

    async def _publisher_loop(self) -> None:
        while True:
            await self._dirty.wait()
            self._dirty.clear()
            await asyncio.sleep(0.4)
            await self._purge_if_needed()
            for company in self.registry.all():
                if not self.registry.is_operational(company):
                    continue
                payload = self.dashboard.build_snapshot(company.slug)
                await self.hub.publish(company.slug, payload)

    async def _run_live_forever(self) -> None:
        force_login = False
        while True:
            retry_delay = 15
            try:
                await self._set_state(connection_state="connecting", last_error=None)
                session = await self.howen.resolve_session(force_login=force_login)
                await self.sync_devices(session.token, force=True)
                await self._run_operational_catchup()
                await self._set_state(connection_state="connected", last_error=None)
                force_login = False

                async for message in self.howen.listen(session):
                    if not isinstance(message, dict):
                        await self._record_ws_message_anomaly(
                            action="__invalid_message__",
                            payload=message,
                            received_at=utc_now(),
                        )
                        continue
                    action = str(message.get("action") or "")
                    received_at = utc_now()
                    raw_payload = message.get("payload")
                    if action in {"__raw_text__", "__non_dict__"}:
                        await self._record_ws_message_anomaly(
                            action=action,
                            payload=raw_payload,
                            received_at=received_at,
                        )
                        continue

                    if action in {"80003", "80004"} and not isinstance(raw_payload, dict):
                        await self._record_ws_message_anomaly(
                            action=action,
                            payload=raw_payload,
                            received_at=received_at,
                        )
                        continue

                    payload = raw_payload if isinstance(raw_payload, dict) else {}
                    payload_text = None if isinstance(raw_payload, dict) or raw_payload in (None, "") else str(raw_payload)

                    if action == "80003":
                        status = self.howen.normalize_status(payload)
                        if not status:
                            await self._record_normalization_failure(
                                source_type="status",
                                payload=payload,
                                received_at=received_at,
                            )
                        elif await self._validate_temporal_integrity(
                            source_type="status",
                            device_id=status.device_id,
                            fleet_id=status.fleet_id,
                            observed_at=status.observed_at,
                            raw_event_time=status.raw_event_time,
                            received_at=received_at,
                            payload=status.raw,
                        ):
                            await self.ingest_status(status, received_at=received_at)
                    elif action == "80004":
                        alarm = self.howen.normalize_alarm(payload)
                        if not alarm:
                            await self._record_normalization_failure(
                                source_type="alarm",
                                payload=payload,
                                received_at=received_at,
                            )
                        elif await self._validate_temporal_integrity(
                            source_type="alarm",
                            device_id=alarm.device_id,
                            fleet_id=alarm.fleet_id,
                            observed_at=alarm.occurred_at,
                            raw_event_time=alarm.raw_event_time,
                            received_at=received_at,
                            payload=alarm.raw,
                        ):
                            await self.ingest_alarm(alarm, received_at=received_at, source="live")
                    elif action == "80000" and ((payload.get("result") or "").lower() == "fail" or (payload_text or "").lower() == "fail"):
                        raise RuntimeError(payload.get("msg") or payload_text or "Howen websocket login failed")
                    elif action == "80009" and self.howen.is_auth_error(payload.get("msg") or payload.get("result") or payload_text or ""):
                        raise RuntimeError(payload.get("msg") or payload_text or "Howen heartbeat rejected the current session")

                    if self._should_sync_devices():
                        await self.sync_devices(session.token, force=False)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.howen.is_auth_error(exc):
                    self.howen.clear_cached_session()
                    force_login = True
                    retry_delay = 10
                elif self.howen.is_login_rate_limited(exc):
                    force_login = True
                    retry_delay = 60
                else:
                    retry_delay = 15
                await self._set_state(connection_state="reconnecting", last_error=str(exc))
                await asyncio.sleep(retry_delay)

    async def backfill_historical(self, request: BackfillRequest) -> dict[str, int]:
        session = await self.howen.resolve_session(force_login=False)
        device_ids: list[str] = []
        if request.device_id:
            device_ids = [request.device_id]
        elif request.company_slug:
            company = self.registry.get(request.company_slug)
            with self.session_factory() as db:
                devices = list(db.scalars(select(DeviceRecord).order_by(DeviceRecord.device_id)))
            device_ids = [
                device.device_id
                for device in devices
                if device.record_source == "live" and self.registry.device_belongs(company, device.device_id, device.fleet_id)
            ]

        inserted = 0
        anomalies = 0
        for device_id in device_ids:
            try:
                rows = await self.howen.fetch_historical_alarms(
                    session.token,
                    device_id=device_id,
                    start_at=request.start_at,
                    end_at=request.end_at,
                )
            except Exception as exc:
                if not self.howen.is_auth_error(exc):
                    raise
                self.howen.clear_cached_session()
                session = await self.howen.resolve_session(force_login=True)
                rows = await self.howen.fetch_historical_alarms(
                    session.token,
                    device_id=device_id,
                    start_at=request.start_at,
                    end_at=request.end_at,
                )
            for row in rows:
                alarm = self.howen.normalize_alarm(row)
                received_at = utc_now()
                if not alarm:
                    if self.howen.is_ignorable_historical_alarm(row):
                        continue
                    anomalies += 1
                    await self._record_normalization_failure(
                        source_type="backfill_alarm",
                        payload=row,
                        received_at=received_at,
                    )
                    continue
                valid = await self._validate_temporal_integrity(
                    source_type="backfill_alarm",
                    device_id=alarm.device_id,
                    fleet_id=alarm.fleet_id,
                    observed_at=alarm.occurred_at,
                    raw_event_time=alarm.raw_event_time,
                    received_at=received_at,
                    payload=alarm.raw,
                )
                if not valid:
                    anomalies += 1
                    continue
                created = await self.ingest_alarm(alarm, received_at=received_at, source="backfill")
                if created:
                    inserted += 1
        return {"inserted": inserted, "anomalies": anomalies, "devices": len(device_ids)}

    async def _run_operational_catchup(self) -> None:
        now_utc = utc_now()
        for company in self.registry.all():
            if not self.registry.is_operational(company):
                continue
            last_run = ensure_utc(self._last_operational_catchup_at.get(company.slug))
            if last_run and now_utc - last_run < timedelta(minutes=30):
                continue
            tz = ZoneInfo(company.timezone or self.settings.default_timezone)
            start_at = datetime.combine(now_utc.astimezone(tz).date(), datetime.min.time(), tzinfo=tz).astimezone(ZoneInfo("UTC"))
            try:
                result = await self.backfill_historical(
                    BackfillRequest(
                        company_slug=company.slug,
                        start_at=start_at,
                        end_at=now_utc,
                    )
                )
            except Exception as exc:
                await self._record_anomaly(
                    source_type="catchup",
                    device_id=None,
                    company_slug=company.slug,
                    received_at=utc_now(),
                    raw_event_time=None,
                    reason=f"catchup_failed:{type(exc).__name__}",
                    payload={
                        "company_slug": company.slug,
                        "error": str(exc),
                        "range_start": start_at.isoformat(),
                        "range_end": now_utc.isoformat(),
                    },
                )
                continue
            self._last_operational_catchup_at[company.slug] = now_utc
            if result["inserted"] or result["anomalies"]:
                self.mark_dirty()

    async def replay_status_anomalies(self, *, limit: int = 2000) -> dict[str, int]:
        with self.session_factory() as session:
            anomalies = list(
                session.scalars(
                    select(IngestionAnomaly)
                    .where(
                        IngestionAnomaly.source_type == "status",
                        IngestionAnomaly.reason == "normalization_failed",
                    )
                    .order_by(IngestionAnomaly.received_at.desc())
                    .limit(limit)
                )
            )

        processed = 0
        inserted = 0
        skipped = 0
        for anomaly in reversed(anomalies):
            try:
                payload = json.loads(anomaly.payload_json)
            except json.JSONDecodeError:
                skipped += 1
                continue
            status = self.howen.normalize_status(payload)
            if not status:
                skipped += 1
                continue
            received_at = ensure_utc(anomaly.received_at) or utc_now()
            valid = await self._validate_temporal_integrity(
                source_type="status_replay",
                device_id=status.device_id,
                fleet_id=status.fleet_id,
                observed_at=status.observed_at,
                raw_event_time=status.raw_event_time,
                received_at=received_at,
                payload=status.raw,
            )
            if not valid:
                skipped += 1
                continue
            await self.ingest_status(status, received_at=received_at, update_feed_state=False)
            processed += 1
            inserted += 1
        return {
            "processed": processed,
            "inserted": inserted,
            "skipped": skipped,
            "loaded": len(anomalies),
        }

    async def sync_devices(self, token: str, *, force: bool) -> None:
        if not force and not self._should_sync_devices():
            return
        rows = await self.howen.fetch_devices(token)
        now = utc_now()
        with self.session_factory() as session:
            for row in rows:
                device_id = str(row.get("deviceno") or row.get("deviceID") or row.get("deviceid") or "").strip()
                if not device_id:
                    continue
                fleet_id = row.get("fleetid") or row.get("fleetId")
                company = self.registry.resolve_company(device_id=device_id, fleet_id=fleet_id)
                record = session.get(DeviceRecord, device_id) or DeviceRecord(device_id=device_id)
                record.plate_no = row.get("plateno") or row.get("plateNo") or row.get("plate") or record.plate_no
                record.company_slug = company.slug if company else record.company_slug
                record.fleet_id = fleet_id or record.fleet_id
                record.fleet_name = row.get("fleetname") or row.get("fleetName") or record.fleet_name
                record.device_name = row.get("devicename") or row.get("deviceName") or record.device_name
                record.record_source = "live"
                record.last_seen_at = record.last_seen_at or now
                session.add(record)
            state = session.get(IngestState, "global")
            if state:
                state.last_device_sync_at = now
            session.commit()
        self._last_device_sync_at = now
        self.mark_dirty()

    async def ingest_status(self, status: NormalizedStatus, *, received_at, update_feed_state: bool = True) -> None:
        received_at = ensure_utc(received_at) or utc_now()
        observed_at = ensure_utc(status.observed_at) or status.observed_at
        company = self.registry.resolve_company(device_id=status.device_id, fleet_id=status.fleet_id)
        company_slug = company.slug if company else None
        timezone_name = self.registry.timezone_for(
            device_id=status.device_id,
            fleet_id=status.fleet_id,
            fallback=self.settings.default_timezone,
        )
        snapshot_date = to_local_date(observed_at, timezone_name)
        anomaly_reasons: list[str] = []
        with self.session_factory() as session:
            record = session.get(DeviceRecord, status.device_id) or DeviceRecord(device_id=status.device_id)
            previous_total_km = record.last_total_km
            validated_total_km = status.total_km
            validated_day_km = status.day_km
            validation_status = "valid"
            validation_reason: str | None = None

            if (
                validated_total_km is not None
                and previous_total_km is not None
                and validated_total_km + 0.1 < previous_total_km
            ):
                validation_status = "invalid"
                validation_reason = "total_regression"
                anomaly_reasons.append(validation_reason)
                validated_total_km = None

            total_reference = validated_total_km if validated_total_km is not None else previous_total_km
            if (
                validated_day_km is not None
                and total_reference is not None
                and validated_day_km > total_reference + 0.1
            ):
                validation_status = "invalid"
                validation_reason = _append_reason(validation_reason, "day_gt_total")
                anomaly_reasons.append("day_gt_total")
                validated_day_km = None

            record.plate_no = status.plate_no or record.plate_no
            record.company_slug = company_slug or record.company_slug
            record.fleet_id = status.fleet_id or record.fleet_id
            record.driver_name = status.driver_name or record.driver_name
            record.device_name = status.device_name or record.device_name
            record.last_seen_at = observed_at
            record.last_received_at = received_at
            if validated_total_km is not None:
                record.last_total_km = validated_total_km
            if validated_day_km is not None:
                record.last_day_km = validated_day_km
            record.record_source = "live"
            record.raw_total_value = status.raw_total_value
            record.raw_day_value = status.raw_day_value
            record.km_validation_status = validation_status
            record.km_validation_reason = validation_reason
            record.raw_payload = json.dumps(status.raw, ensure_ascii=True)
            session.add(record)

            snapshot = session.scalar(
                select(DailyMileageSnapshot).where(
                    DailyMileageSnapshot.device_id == status.device_id,
                    DailyMileageSnapshot.snapshot_date == snapshot_date,
                )
            )
            if not snapshot:
                snapshot = DailyMileageSnapshot(
                    device_id=status.device_id,
                    snapshot_date=snapshot_date,
                    observed_at=observed_at,
                    total_km=validated_total_km or record.last_total_km or 0.0,
                    day_km=validated_day_km,
                    plate_no=record.plate_no,
                    company_slug=record.company_slug,
                    fleet_id=record.fleet_id,
                    raw_total_value=status.raw_total_value,
                    raw_day_value=status.raw_day_value,
                    km_validation_status=validation_status,
                    km_validation_reason=validation_reason,
                    source="live",
                )
            else:
                snapshot.observed_at = observed_at
                snapshot.plate_no = record.plate_no
                snapshot.company_slug = record.company_slug
                snapshot.fleet_id = record.fleet_id
                snapshot.raw_total_value = status.raw_total_value
                snapshot.raw_day_value = status.raw_day_value
                snapshot.km_validation_status = validation_status
                snapshot.km_validation_reason = validation_reason
                snapshot.source = "live"
                if validated_total_km is not None:
                    snapshot.total_km = validated_total_km
                if validated_day_km is not None:
                    snapshot.day_km = validated_day_km
            session.add(snapshot)

            state = session.get(IngestState, "global")
            if state and update_feed_state:
                state.mode = "live"
                state.connection_state = "connected"
                state.last_message_at = _max_datetime(state.last_message_at, received_at)
                state.last_cycle_received_at = _max_datetime(state.last_cycle_received_at, received_at)
                state.last_event_observed_at = _max_datetime(state.last_event_observed_at, observed_at)
                state.last_status_at = _max_datetime(state.last_status_at, observed_at)
                state.last_error = None
            session.commit()
        for reason in anomaly_reasons:
            await self._record_anomaly(
                source_type="status",
                device_id=status.device_id,
                company_slug=company_slug,
                received_at=received_at,
                raw_event_time=status.raw_event_time,
                reason=reason,
                payload=status.raw,
            )
        self.mark_dirty()

    async def ingest_alarm(self, alarm: NormalizedAlarm, *, received_at, source: str) -> bool:
        received_at = ensure_utc(received_at) or utc_now()
        occurred_at = ensure_utc(alarm.occurred_at) or alarm.occurred_at
        company = self.registry.resolve_company(device_id=alarm.device_id, fleet_id=alarm.fleet_id)
        company_slug = company.slug if company else None
        timezone_name = self.registry.timezone_for(
            device_id=alarm.device_id,
            fleet_id=alarm.fleet_id,
            fallback=self.settings.default_timezone,
        )
        snapshot_date = to_local_date(occurred_at, timezone_name)
        with self.session_factory() as session:
            if session.get(AlarmEvent, alarm.guid):
                return False
            record = session.get(DeviceRecord, alarm.device_id)
            plate_no = alarm.plate_no or (record.plate_no if record else None)
            fleet_id = alarm.fleet_id or (record.fleet_id if record else None)
            driver_name = alarm.driver_name or (record.driver_name if record else None)
            if not record:
                record = DeviceRecord(device_id=alarm.device_id)
            record.plate_no = plate_no or record.plate_no
            record.company_slug = company_slug or record.company_slug
            record.fleet_id = fleet_id or record.fleet_id
            record.driver_name = driver_name or record.driver_name
            record.last_seen_at = _max_datetime(record.last_seen_at, occurred_at)
            if alarm.total_mileage_km is not None:
                if record.last_total_km is None or record.last_total_km > alarm.total_mileage_km or alarm.total_mileage_km >= record.last_total_km:
                    record.last_total_km = alarm.total_mileage_km
            session.add(record)

            session.add(
                AlarmEvent(
                    guid=alarm.guid,
                    device_id=alarm.device_id,
                    plate_no=plate_no,
                    company_slug=company_slug,
                    fleet_id=fleet_id,
                    driver_name=driver_name,
                    category=alarm.category,
                    subtype=alarm.subtype,
                    mapping_source=alarm.mapping_source,
                    classification_status=alarm.classification_status,
                    visibility_status=alarm.visibility_status,
                    event_code=alarm.event_code,
                    raw_alarm_type=alarm.raw_alarm_type,
                    raw_tp=alarm.raw_tp,
                    raw_event_code=alarm.raw_event_code,
                    occurred_at=occurred_at,
                    received_at=received_at,
                    start_at=alarm.start_at,
                    end_at=alarm.end_at,
                    raw_event_time=alarm.raw_event_time,
                    latitude=alarm.latitude,
                    longitude=alarm.longitude,
                    total_mileage_km=alarm.total_mileage_km,
                    source=source,
                    raw_payload=json.dumps(alarm.raw, ensure_ascii=True),
                )
            )
            if alarm.classification_status != "classified_dms":
                self._append_alarm_audit(
                    session,
                    guid=alarm.guid,
                    company_slug=company_slug,
                    device_id=alarm.device_id,
                    fleet_id=fleet_id,
                    plate_no=plate_no,
                    observed_at=occurred_at,
                    received_at=received_at,
                    raw_alarm_type=alarm.raw_alarm_type,
                    raw_tp=alarm.raw_tp,
                    raw_event_code=alarm.raw_event_code,
                    stage="classification",
                    reason=alarm.classification_status,
                    payload=alarm.raw,
                )

            if alarm.total_mileage_km is not None:
                snapshot = session.scalar(
                    select(DailyMileageSnapshot).where(
                        DailyMileageSnapshot.device_id == alarm.device_id,
                        DailyMileageSnapshot.snapshot_date == snapshot_date,
                    )
                )
                if not snapshot:
                    snapshot = DailyMileageSnapshot(
                        device_id=alarm.device_id,
                        snapshot_date=snapshot_date,
                        observed_at=occurred_at,
                        total_km=alarm.total_mileage_km,
                        day_km=None,
                        plate_no=plate_no,
                        company_slug=company_slug,
                        fleet_id=fleet_id,
                        raw_total_value=_payload_value(alarm.raw, "totalMileage", "total"),
                        source=source,
                    )
                else:
                    if occurred_at >= (ensure_utc(snapshot.observed_at) or occurred_at):
                        snapshot.observed_at = occurred_at
                        snapshot.total_km = alarm.total_mileage_km
                    snapshot.plate_no = plate_no or snapshot.plate_no
                    snapshot.company_slug = company_slug or snapshot.company_slug
                    snapshot.fleet_id = fleet_id or snapshot.fleet_id
                    snapshot.raw_total_value = _payload_value(alarm.raw, "totalMileage", "total")
                    if snapshot.source != "live":
                        snapshot.source = source
                session.add(snapshot)

            state = session.get(IngestState, "global")
            if state and source == "live":
                state.mode = "live"
                state.connection_state = "connected"
                state.last_message_at = _max_datetime(state.last_message_at, received_at)
                state.last_cycle_received_at = _max_datetime(state.last_cycle_received_at, received_at)
                state.last_event_observed_at = _max_datetime(state.last_event_observed_at, occurred_at)
                state.last_alarm_at = _max_datetime(state.last_alarm_at, occurred_at)
                state.last_error = None
            session.commit()
        self.mark_dirty()
        return True

    def _ensure_state_row(self) -> None:
        with self.session_factory() as session:
            if not session.get(IngestState, "global"):
                session.add(IngestState(key="global", mode="live", connection_state="starting"))
                session.commit()

    async def _set_state(self, *, connection_state: str, last_error: str | None) -> None:
        with self.session_factory() as session:
            state = session.get(IngestState, "global")
            if not state:
                state = IngestState(key="global")
                session.add(state)
            state.mode = "live"
            state.connection_state = connection_state
            state.last_error = last_error
            session.commit()
        self.mark_dirty()

    async def _validate_temporal_integrity(
        self,
        *,
        source_type: str,
        device_id: str | None,
        fleet_id: str | None,
        observed_at,
        raw_event_time: str | None,
        received_at,
        payload: dict[str, Any],
    ) -> bool:
        observed_at = ensure_utc(observed_at)
        received_at = ensure_utc(received_at) or utc_now()
        if observed_at is None:
            return False
        tolerance = timedelta(minutes=self.settings.anomaly_future_tolerance_minutes)
        if observed_at - received_at <= tolerance:
            return True
        company = self.registry.resolve_company(device_id=device_id, fleet_id=fleet_id)
        await self._record_anomaly(
            source_type=source_type,
            device_id=device_id,
            company_slug=company.slug if company else None,
            received_at=received_at,
            raw_event_time=raw_event_time,
            reason="future_timestamp",
            payload=payload,
        )
        return False

    async def _record_normalization_failure(
        self,
        *,
        source_type: str,
        payload: dict[str, Any],
        received_at,
    ) -> None:
        device_id = _payload_value(payload, "deviceID", "deviceno", "deviceid")
        fleet_id = _payload_value(payload, "fleetID", "fleetId", "fleetid")
        company = self.registry.resolve_company(device_id=device_id, fleet_id=fleet_id)
        raw_event_time = _payload_value(
            payload,
            "dtu",
            "reportTime",
            "st",
            "et",
        )
        await self._record_anomaly(
            source_type=source_type,
            device_id=device_id,
            company_slug=company.slug if company else None,
            received_at=received_at,
            raw_event_time=raw_event_time,
            reason="normalization_failed",
            payload=payload,
        )

    async def _record_ws_message_anomaly(
        self,
        *,
        action: str,
        payload: Any,
        received_at,
    ) -> None:
        normalized_payload = payload if isinstance(payload, dict) else {"action": action, "raw_payload": payload, "raw_type": type(payload).__name__}
        await self._record_anomaly(
            source_type="ws_message",
            device_id=None,
            company_slug=None,
            received_at=received_at,
            raw_event_time=None,
            reason="unexpected_message_shape",
            payload=normalized_payload,
        )

    async def _record_anomaly(
        self,
        *,
        source_type: str,
        device_id: str | None,
        company_slug: str | None,
        received_at,
        raw_event_time: str | None,
        reason: str,
        payload: dict[str, Any],
    ) -> None:
        with self.session_factory() as session:
            raw_alarm_type = _payload_alarm_type(payload)
            raw_tp = _payload_alarm_tp(payload)
            raw_event_code = _payload_alarm_event_code(payload)
            plate_no = _payload_value(payload, "plateNo", "plateno", "plate")
            fleet_id = _payload_value(payload, "fleetID", "fleetId", "fleetid")
            session.add(
                IngestionAnomaly(
                    source_type=source_type,
                    device_id=device_id,
                    company_slug=company_slug,
                    received_at=received_at,
                    raw_event_time=raw_event_time,
                    reason=reason,
                    payload_json=json.dumps(payload, ensure_ascii=True),
                )
            )
            self._append_alarm_audit(
                session,
                guid=_payload_value(payload, "alarmID", "guid", "uuid"),
                company_slug=company_slug,
                device_id=device_id,
                fleet_id=fleet_id,
                plate_no=plate_no,
                observed_at=None,
                received_at=received_at,
                raw_alarm_type=raw_alarm_type,
                raw_tp=raw_tp,
                raw_event_code=raw_event_code,
                stage=source_type,
                reason=reason,
                payload=payload,
            )
            state = session.get(IngestState, "global")
            if state:
                state.last_anomaly_at = received_at
            session.commit()
        self.mark_dirty()

    def _append_alarm_audit(
        self,
        session,
        *,
        guid: str | None,
        company_slug: str | None,
        device_id: str | None,
        fleet_id: str | None,
        plate_no: str | None,
        observed_at,
        received_at,
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

    async def _purge_if_needed(self) -> None:
        now = utc_now()
        if self._last_purge_at and now - self._last_purge_at < timedelta(hours=1):
            return
        live_cutoff = now - timedelta(days=self.settings.live_retention_days)
        anomaly_cutoff = now - timedelta(days=self.settings.anomaly_retention_days)
        with self.session_factory() as session:
            session.execute(delete(AlarmEvent).where(AlarmEvent.occurred_at < live_cutoff))
            session.execute(delete(MileageReading).where(MileageReading.recorded_at < live_cutoff))
            session.execute(delete(DailyMileageSnapshot).where(DailyMileageSnapshot.observed_at < live_cutoff))
            session.execute(delete(IngestionAnomaly).where(IngestionAnomaly.received_at < anomaly_cutoff))
            session.execute(delete(AlarmEventAudit).where(AlarmEventAudit.received_at < anomaly_cutoff))
            session.commit()
        self._last_purge_at = now

    def _should_sync_devices(self) -> bool:
        if not self._last_device_sync_at:
            return True
        return utc_now() - self._last_device_sync_at >= timedelta(hours=24)


def _max_datetime(left, right):
    left = ensure_utc(left)
    right = ensure_utc(right)
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def _payload_value(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    for key in ("payload", "ext", "location", "det"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            nested_value = _payload_value(nested, *keys)
            if nested_value:
                return nested_value
    return None


def _payload_alarm_type(payload: dict[str, Any]) -> str | None:
    return _payload_value(payload, "alarmTypeValue", "alarmType", "alarmtypeValue")


def _payload_alarm_tp(payload: dict[str, Any]) -> str | None:
    direct = _payload_value(payload, "tp")
    if direct:
        return direct
    alarm_detail = _payload_value(payload, "alarmvalue", "alarmDetail")
    if not alarm_detail:
        return None
    marker = "tp:"
    if marker not in alarm_detail:
        return None
    tail = alarm_detail.split(marker, 1)[1]
    digits = ""
    for char in tail:
        if char.isdigit():
            digits += char
            continue
        break
    return digits or None


def _payload_alarm_event_code(payload: dict[str, Any]) -> str | None:
    return _payload_value(payload, "ec", "alarmtype", "alarmType")


def _append_reason(current: str | None, reason: str) -> str:
    if not current:
        return reason
    reasons = [item for item in current.split(",") if item]
    if reason in reasons:
        return current
    reasons.append(reason)
    return ",".join(reasons)
