from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.database import Base
from app.core.time import utc_now
from app.models import AlarmEvent, AlarmEventAudit, CompanyHistoricalRebuildJob, DailyMileageSnapshot, DeviceRecord, HowenAlarmRaw, IngestionAnomaly
from app.schemas import NormalizedAlarm
from app.services.ingestion import IngestionService


class _Registry:
    def __init__(self) -> None:
        self.company = SimpleNamespace(slug="test-company", timezone="America/Bogota")

    def resolve_company(self, **_kwargs):
        return self.company

    def get(self, slug: str):
        if slug != self.company.slug:
            raise KeyError(slug)
        return self.company

    def normalize_plate_any(self, value):
        return str(value).upper() if value else None

    def normalize_plate(self, _company, value):
        return self.normalize_plate_any(value)

    def canonical_plate(self, device_id, *candidates):
        for candidate in candidates:
            if candidate and str(candidate).upper() != str(device_id).upper():
                return str(candidate).upper()
        return str(device_id).upper() if device_id else None

    def timezone_for(self, **_kwargs):
        return "America/Bogota"


class _Dashboard:
    def clear_runtime_caches(self) -> None:
        return None


class _DirtyFlag:
    def set(self) -> None:
        return None


class IngestionAlarmBatchTests(unittest.TestCase):
    def setUp(self) -> None:
        engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            future=True,
        )
        Base.metadata.create_all(engine)
        self.session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
        self.service = IngestionService.__new__(IngestionService)
        self.service.settings = SimpleNamespace(
            anomaly_future_tolerance_minutes=5,
            default_timezone="America/Bogota",
            historical_batch_mode="activation_only",
            historical_batch_size=2,
        )
        self.service.session_factory = self.session_factory
        self.service.registry = _Registry()
        self.service.dashboard = _Dashboard()
        self.service._dirty = _DirtyFlag()

    def _new_service(self):
        engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            future=True,
        )
        Base.metadata.create_all(engine)
        session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
        service = IngestionService.__new__(IngestionService)
        service.settings = self.service.settings
        service.session_factory = session_factory
        service.registry = _Registry()
        service.dashboard = _Dashboard()
        service._dirty = _DirtyFlag()
        return service, session_factory

    def _alarm(
        self,
        *,
        guid: str,
        occurred_at,
        category: str = "Ojos cerrados",
        classification_status: str = "classified_dms",
        raw_alarm_type: str = "Eyes Closed",
        raw_event_code: str = "104",
    ) -> NormalizedAlarm:
        return NormalizedAlarm(
            guid=guid,
            device_id="device-1",
            occurred_at=occurred_at,
            category=category,
            mapping_source="text_alarm_type",
            raw_alarm_type=raw_alarm_type,
            raw_event_code=raw_event_code,
            classification_status=classification_status,
            visibility_status="candidate" if classification_status == "classified_dms" else "hidden_non_dms",
            plate_no="ABC123",
            fleet_id="fleet-1",
            total_mileage_km=999.0,
            raw_event_time=occurred_at.isoformat(),
            raw={
                "alarmID": guid,
                "deviceID": "device-1",
                "plateNo": "ABC123",
                "alarmTypeValue": raw_alarm_type,
                "alarmType": raw_event_code,
                "reportTime": occurred_at.isoformat(),
                "totalMileage": 999000,
            },
        )

    def test_batch_is_idempotent_and_keeps_layers_separate(self) -> None:
        now = utc_now().replace(microsecond=0)
        alarms = [
            self._alarm(guid="dms-1", occurred_at=now - timedelta(minutes=30)),
            self._alarm(
                guid="dms-duplicate",
                occurred_at=now - timedelta(minutes=30),
            ),
            self._alarm(
                guid="non-dms-1",
                occurred_at=now - timedelta(minutes=20),
                category="No DMS",
                classification_status="classified_non_dms",
                raw_alarm_type="Ignition On",
                raw_event_code="31",
            ),
            self._alarm(
                guid="future-1",
                occurred_at=now + timedelta(minutes=20),
            ),
        ]

        first = asyncio.run(
            self.service.ingest_alarm_batch(
                alarms,
                source="harvest",
                company=self.service.registry.company,
                batch_size=2,
            )
        )
        second = asyncio.run(
            self.service.ingest_alarm_batch(
                alarms,
                source="harvest",
                company=self.service.registry.company,
                batch_size=2,
            )
        )

        with self.session_factory() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(HowenAlarmRaw)), 3)
            self.assertEqual(session.scalar(select(func.count()).select_from(AlarmEvent)), 1)
            self.assertEqual(session.scalar(select(func.count()).select_from(IngestionAnomaly)), 1)
            self.assertGreater(session.scalar(select(func.count()).select_from(AlarmEventAudit)), 0)
            self.assertEqual(session.scalar(select(func.count()).select_from(DailyMileageSnapshot)), 0)
            device = session.get(DeviceRecord, "device-1")
            self.assertIsNotNone(device)
            self.assertIsNone(device.last_total_km)

        self.assertEqual(first.provider_rows, 4)
        self.assertEqual(first.prepared_rows, 3)
        self.assertEqual(first.duplicates, 1)
        self.assertEqual(first.raw_inserted, 3)
        self.assertEqual(first.dms_inserted, 1)
        self.assertEqual(first.temporal_rejected, 1)
        self.assertEqual(second.raw_inserted, 0)
        self.assertEqual(second.raw_updated, 3)
        self.assertEqual(second.dms_inserted, 0)
        self.assertEqual(second.dms_updated, 1)

    def test_batch_updates_rebuild_progress_per_chunk(self) -> None:
        now = utc_now().replace(microsecond=0)
        with self.session_factory() as session:
            job = CompanyHistoricalRebuildJob(
                company_slug="test-company",
                purpose="activation_bootstrap",
                status="running",
                start_date=now.date(),
                end_date=now.date(),
            )
            session.add(job)
            session.commit()
            job_id = job.id

        alarms = [
            self._alarm(guid=f"dms-{index}", occurred_at=now - timedelta(minutes=index + 10))
            for index in range(5)
        ]
        result = asyncio.run(
            self.service.ingest_alarm_batch(
                alarms,
                source="harvest",
                company=self.service.registry.company,
                batch_size=2,
                rebuild_job_id=job_id,
            )
        )

        with self.session_factory() as session:
            job = session.get(CompanyHistoricalRebuildJob, job_id)
            self.assertEqual(job.rows_total, 5)
            self.assertEqual(job.rows_processed, 5)
            self.assertEqual(job.phase, "fetching")
            self.assertIsNone(job.current_device_id)
            self.assertIsNotNone(job.last_heartbeat_at)
        self.assertEqual(result.chunks_committed, 3)

    def test_batch_prefers_canonical_device_plate_over_numeric_provider_label(self) -> None:
        now = utc_now().replace(microsecond=0)
        alarm = self._alarm(guid="canonical-plate", occurred_at=now - timedelta(minutes=10))
        alarm.plate_no = "867869064064439"

        asyncio.run(
            self.service.ingest_alarm_batch(
                [alarm],
                source="harvest",
                company=self.service.registry.company,
                device_context={"plate_no": "TTR888"},
            )
        )

        with self.session_factory() as session:
            raw = session.scalar(select(HowenAlarmRaw))
            analytic = session.scalar(select(AlarmEvent))
            self.assertEqual(raw.plate_no, "TTR888")
            self.assertEqual(analytic.plate_no, "TTR888")

    def test_company_assignment_propagates_with_bulk_updates(self) -> None:
        now = utc_now().replace(microsecond=0)
        with self.session_factory() as session:
            session.add_all(
                [
                    HowenAlarmRaw(
                        guid="raw-assignment",
                        device_id="device-1",
                        plate_no="867869064064439",
                        source="harvest",
                        received_at=now,
                        payload_json="{}",
                    ),
                    AlarmEvent(
                        guid="alarm-assignment",
                        device_id="device-1",
                        plate_no="867869064064439",
                        category="Ojos cerrados",
                        occurred_at=now,
                        source="harvest",
                    ),
                    DailyMileageSnapshot(
                        device_id="device-1",
                        plate_no="867869064064439",
                        snapshot_date=now.date(),
                        total_km=1000.0,
                        day_km=12.5,
                        observed_at=now,
                        source="status",
                    ),
                ]
            )
            session.commit()

        with self.session_factory() as session:
            self.service._propagate_company_assignment(
                session,
                device_id="device-1",
                company_slug="test-company",
                plate_no="TTR888",
                fleet_id="fleet-1",
            )
            session.commit()

        with self.session_factory() as session:
            rows = [
                session.get(HowenAlarmRaw, "raw-assignment"),
                session.get(AlarmEvent, "alarm-assignment"),
                session.scalar(select(DailyMileageSnapshot)),
            ]
            for row in rows:
                self.assertEqual(row.company_slug, "test-company")
                self.assertEqual(row.plate_no, "TTR888")
                self.assertEqual(row.fleet_id, "fleet-1")
            self.assertEqual(rows[-1].day_km, 12.5)

    def test_batch_matches_individual_pipeline_for_raw_and_analytic_rows(self) -> None:
        now = utc_now().replace(microsecond=0)
        alarms = [
            self._alarm(guid="dms-compare", occurred_at=now - timedelta(hours=2)),
            self._alarm(
                guid="non-dms-compare",
                occurred_at=now - timedelta(hours=1),
                category="No DMS",
                classification_status="classified_non_dms",
                raw_alarm_type="Ignition Off",
                raw_event_code="19",
            ),
        ]
        asyncio.run(
            self.service.ingest_alarm_batch(
                alarms,
                source="harvest",
                company=self.service.registry.company,
                batch_size=500,
            )
        )

        individual_service, individual_sessions = self._new_service()
        for alarm in alarms:
            asyncio.run(individual_service.ingest_alarm(alarm, received_at=utc_now(), source="harvest"))

        def read_projection(session_factory):
            with session_factory() as session:
                raw = session.execute(
                    select(
                        HowenAlarmRaw.provider_event_key,
                        HowenAlarmRaw.device_id,
                        HowenAlarmRaw.classification_status,
                        HowenAlarmRaw.mapped_category,
                        HowenAlarmRaw.temporal_status,
                        HowenAlarmRaw.ingest_result,
                    ).order_by(HowenAlarmRaw.provider_event_key)
                ).all()
                analytic = session.execute(
                    select(
                        AlarmEvent.provider_event_key,
                        AlarmEvent.device_id,
                        AlarmEvent.category,
                        AlarmEvent.classification_status,
                        AlarmEvent.visibility_status,
                    ).order_by(AlarmEvent.provider_event_key)
                ).all()
                return raw, analytic

        self.assertEqual(read_projection(self.session_factory), read_projection(individual_sessions))

    def test_failure_rolls_back_only_the_current_chunk(self) -> None:
        now = utc_now().replace(microsecond=0)
        alarms = [
            self._alarm(guid=f"dms-failure-{index}", occurred_at=now - timedelta(minutes=index + 10))
            for index in range(4)
        ]

        from app.services import ingestion as ingestion_module

        original_upsert = ingestion_module._bulk_upsert_rows
        upsert_calls = 0

        def fail_on_second_chunk(*args, **kwargs):
            nonlocal upsert_calls
            upsert_calls += 1
            if upsert_calls == 3:
                raise RuntimeError("simulated second chunk failure")
            return original_upsert(*args, **kwargs)

        with patch("app.services.ingestion._bulk_upsert_rows", side_effect=fail_on_second_chunk):
            with self.assertRaisesRegex(RuntimeError, "simulated second chunk failure"):
                asyncio.run(
                    self.service.ingest_alarm_batch(
                        alarms,
                        source="harvest",
                        company=self.service.registry.company,
                        batch_size=2,
                    )
                )

        with self.session_factory() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(HowenAlarmRaw)), 2)
            self.assertEqual(session.scalar(select(func.count()).select_from(AlarmEvent)), 2)

        retry = asyncio.run(
            self.service.ingest_alarm_batch(
                alarms,
                source="harvest",
                company=self.service.registry.company,
                batch_size=2,
            )
        )
        self.assertEqual(retry.raw_inserted, 2)
        self.assertEqual(retry.raw_updated, 2)
        self.assertEqual(retry.dms_inserted, 2)
        self.assertEqual(retry.dms_updated, 2)


if __name__ == "__main__":
    unittest.main()
