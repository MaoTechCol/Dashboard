from __future__ import annotations

import asyncio
from datetime import date
from types import SimpleNamespace
import unittest
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.database import Base
from app.models import DailyMileageSnapshot, DeviceRecord, MileageObservation, ReconciliationReview
from app.services.ingestion import IngestionService, _mileage_report_daily_values


class _Howen:
    async def fetch_daily_mileage_report_authorized(self, **_kwargs):
        return [
            {
                "deviceName": "TTR888(867869064064439)",
                "2026-08-20": "12.5",
            }
        ]


class _Dashboard:
    def clear_runtime_caches(self) -> None:
        return None


class AuthoritativeMileageTests(unittest.TestCase):
    def setUp(self) -> None:
        engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            future=True,
        )
        Base.metadata.create_all(engine)
        self.session_factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)
        self.service = IngestionService.__new__(IngestionService)
        self.service.session_factory = self.session_factory
        self.service.howen = _Howen()
        self.service.dashboard = _Dashboard()
        self.service.registry = SimpleNamespace(
            get=lambda _slug: SimpleNamespace(timezone="America/Bogota")
        )
        self.service.settings = SimpleNamespace(
            default_timezone="America/Bogota",
            backfill_rate_limit_cooldown_seconds=60,
            mileage_source_disagreement_tolerance_km=1.0,
            mileage_max_daily_km=1_500.0,
            mileage_rebuild_min_coverage_pct=90.0,
        )
        with self.session_factory() as session:
            session.add(
                DeviceRecord(
                    device_id="867869064064439",
                    company_slug="comerpolsas",
                    fleet_id="fleet-1",
                    plate_no="TTR888",
                )
            )
            session.commit()

    def test_missing_provider_day_is_reviewed_and_never_materialized_as_zero(self) -> None:
        result = asyncio.run(
            self.service.rebuild_authoritative_mileage(
                company_slug="comerpolsas",
                start_date_local=date(2026, 8, 20),
                end_date_local=date(2026, 8, 21),
                company_tz=ZoneInfo("America/Bogota"),
                device_ids=["867869064064439"],
                rebuild_job_id=None,
            )
        )

        self.assertEqual(result.expected_device_days, 2)
        self.assertEqual(result.valid_device_days, 1)
        self.assertEqual(result.missing_device_days, 1)
        self.assertEqual(result.coverage_pct, 50.0)
        self.assertFalse(result.publication_allowed)

        with self.session_factory() as session:
            snapshots = list(session.scalars(select(DailyMileageSnapshot)))
            observations = list(session.scalars(select(MileageObservation)))
            reviews = list(session.scalars(select(ReconciliationReview)))

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].snapshot_date, date(2026, 8, 20))
        self.assertEqual(snapshots[0].day_km, 12.5)
        self.assertEqual(len(observations), 1)
        self.assertEqual(len(reviews), 1)
        self.assertEqual(reviews[0].reason, "missing_day_km")

    def test_daily_report_parser_accepts_nested_rows_and_real_zero(self) -> None:
        values = _mileage_report_daily_values(
            {
                "dailyMileageList": [
                    {"date": "2026-08-20", "mileage": 0},
                    {"date": "2026-08-21", "mileage": "17.25 km"},
                ]
            },
            start_date=date(2026, 8, 20),
            end_date=date(2026, 8, 21),
        )

        self.assertEqual(values[date(2026, 8, 20)][0], 0.0)
        self.assertEqual(values[date(2026, 8, 21)][0], 17.25)


if __name__ == "__main__":
    unittest.main()
