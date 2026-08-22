from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.database import Base
from app.models import (
    BackgroundJob,
    CompanyDailyAggregate,
    CompanyWindowAggregate,
    DataCertificationRun,
    DeviceRecord,
    PublishedDashboardSnapshot,
)
from app.services.ingestion import IngestionService


class _Registry:
    company = SimpleNamespace(
        slug="alpha",
        timezone="America/Bogota",
        device_ids=["device-1"],
        fleet_ids=["fleet-1"],
    )

    def get(self, slug: str):
        if slug != self.company.slug:
            raise KeyError(slug)
        return self.company

    @staticmethod
    def device_belongs(company, device_id: str, fleet_id: str | None) -> bool:
        return device_id in company.device_ids or fleet_id in company.fleet_ids


class CompanyPurgeConsistencyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            future=True,
        )
        Base.metadata.create_all(engine)
        self.sessions = sessionmaker(bind=engine, autoflush=False, expire_on_commit=True, future=True)
        self.temp_dir = TemporaryDirectory()
        self.service = IngestionService.__new__(IngestionService)
        self.service.session_factory = self.sessions
        self.service.registry = _Registry()
        self.service.settings = SimpleNamespace(upload_dir=Path(self.temp_dir.name))
        self.service.dashboard = SimpleNamespace(clear_runtime_caches=lambda: None)

        async def set_maintenance_mode(*, enabled: bool, reason: str | None):
            return {"enabled": enabled, "reason": reason}

        self.service.set_maintenance_mode = set_maintenance_mode

    async def asyncTearDown(self) -> None:
        self.temp_dir.cleanup()

    async def test_purge_removes_all_derived_layers_but_keeps_provider_catalog_device(self) -> None:
        now = datetime(2026, 8, 22, 12, 0, tzinfo=ZoneInfo("UTC"))
        with self.sessions() as session:
            session.add(
                DeviceRecord(
                    device_id="device-1",
                    company_slug="alpha",
                    fleet_id="fleet-1",
                    record_source="live",
                )
            )
            session.add(
                CompanyDailyAggregate(
                    company_slug="alpha",
                    aggregate_date=date(2026, 8, 22),
                    metrics_json="{}",
                )
            )
            session.add(
                CompanyWindowAggregate(
                    company_slug="alpha",
                    window_type="24h",
                    range_start=now,
                    range_end=now,
                    snapshot_version="v1",
                    payload_json="{}",
                )
            )
            session.add(
                DataCertificationRun(
                    id="cert-1",
                    company_slug="alpha",
                    source_name="benchmark.xlsx",
                    range_start=now,
                    range_end=now,
                    status="completed",
                )
            )
            session.add(PublishedDashboardSnapshot(company_slug="alpha", snapshot_json="{}"))
            session.add(
                BackgroundJob(
                    id="old-job",
                    job_type="historical_rebuild",
                    company_slug="alpha",
                    status="succeeded",
                    payload_json="{}",
                    idempotency_key="old-job",
                )
            )
            session.add(
                BackgroundJob(
                    id="purge-job",
                    job_type="company_purge",
                    company_slug="alpha",
                    status="running",
                    payload_json="{}",
                    idempotency_key="purge-job",
                )
            )
            session.commit()

        result = await self.service.purge_company_operational_data(company_slug="alpha")

        self.assertEqual(result["company_daily_aggregates"], 1)
        self.assertEqual(result["company_window_aggregates"], 1)
        self.assertEqual(result["data_certification_runs"], 1)
        self.assertEqual(result["background_jobs"], 1)
        with self.sessions() as session:
            self.assertIsNotNone(session.get(DeviceRecord, "device-1"))
            self.assertIsNone(session.get(BackgroundJob, "old-job"))
            self.assertIsNotNone(session.get(BackgroundJob, "purge-job"))
            self.assertIsNone(session.scalar(select(CompanyDailyAggregate)))
            self.assertIsNone(session.scalar(select(CompanyWindowAggregate)))
            self.assertIsNone(session.scalar(select(DataCertificationRun)))


if __name__ == "__main__":
    unittest.main()
