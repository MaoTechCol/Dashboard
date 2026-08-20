from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.database import Base
from app.models import AlarmEvent, MileageReading
from app.services.dashboard import _build_daily_km, _load_legacy_daily_km


class DashboardKmSourceTests(unittest.TestCase):
    def test_sparse_alarm_odometer_does_not_create_daily_km(self) -> None:
        occurred_at = datetime(2026, 8, 10, 15, 0, tzinfo=timezone.utc)
        event = AlarmEvent(
            guid="alarm-km-sample",
            device_id="device-1",
            plate_no="ABC123",
            category="Ojos cerrados",
            occurred_at=occurred_at,
            received_at=occurred_at,
            total_mileage_km=1500.0,
            source="harvest",
        )

        by_vehicle, by_date = _build_daily_km([], [], [event], ZoneInfo("America/Bogota"))

        self.assertEqual(dict(by_vehicle), {})
        self.assertEqual(dict(by_date), {})

    def test_status_observations_are_aggregated_by_company_and_day(self) -> None:
        engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            future=True,
        )
        Base.metadata.create_all(engine)
        sessions = sessionmaker(bind=engine, future=True)
        start = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
        with sessions() as session:
            session.add_all(
                [
                    MileageReading(device_id="device-1", plate_no="ABC123", fleet_id="fleet-1", recorded_at=start, total_km=100.0, day_km=5.0),
                    MileageReading(device_id="device-1", plate_no="ABC123", fleet_id="fleet-1", recorded_at=start + timedelta(hours=1), total_km=110.0, day_km=7.0),
                    MileageReading(device_id="device-2", plate_no="DEF456", fleet_id="fleet-1", recorded_at=start, total_km=200.0, day_km=None),
                    MileageReading(device_id="device-2", plate_no="DEF456", fleet_id="fleet-1", recorded_at=start + timedelta(hours=1), total_km=215.0, day_km=None),
                    MileageReading(device_id="other", plate_no="ZZZ999", fleet_id="fleet-2", recorded_at=start, total_km=50.0, day_km=50.0),
                ]
            )
            session.commit()
            rows = _load_legacy_daily_km(
                session,
                company=SimpleNamespace(slug="company-1", timezone="America/Bogota", device_ids=[], fleet_ids=["fleet-1"]),
                cutoff=start - timedelta(days=1),
                reference_utc=start + timedelta(days=1),
            )

        by_device = {device_id: day_km for device_id, _plate, _day, day_km in rows}
        self.assertEqual(by_device, {"device-1": 7.0, "device-2": 15.0})


if __name__ == "__main__":
    unittest.main()
