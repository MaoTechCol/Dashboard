from __future__ import annotations

from datetime import datetime, timezone
import unittest
from zoneinfo import ZoneInfo

from app.models import AlarmEvent
from app.services.dashboard import _build_daily_km


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


if __name__ == "__main__":
    unittest.main()
