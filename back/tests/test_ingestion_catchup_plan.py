from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import unittest

from app.services.ingestion import CatchupPlan, IngestionService


class IngestionCatchupPlanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = IngestionService.__new__(IngestionService)
        self.utc = ZoneInfo("UTC")

    def test_recent_cursor_keeps_recent_overlap_scan(self) -> None:
        now_utc = datetime(2026, 8, 16, 3, 15, tzinfo=self.utc)
        last_successful_cursor = datetime(2026, 8, 16, 3, 5, tzinfo=self.utc)

        plan = self.service._plan_operational_catchup(
            force=False,
            now_utc=now_utc,
            last_successful_cursor=last_successful_cursor,
            pending_range_start_at=None,
            pending_range_end_at=None,
            next_device_offset=0,
            next_retry_at=None,
            overlap=timedelta(minutes=10),
            stale_after=timedelta(minutes=90),
            bootstrap_span=timedelta(hours=6),
            effective_window=timedelta(minutes=20),
        )

        self.assertEqual(
            plan,
            CatchupPlan(
                start_at=datetime(2026, 8, 16, 2, 55, tzinfo=self.utc),
                end_at=now_utc,
                offset=0,
            ),
        )

    def test_stale_cursor_advances_forward_window(self) -> None:
        now_utc = datetime(2026, 8, 16, 3, 15, tzinfo=self.utc)
        last_successful_cursor = datetime(2026, 8, 16, 0, 0, tzinfo=self.utc)

        plan = self.service._plan_operational_catchup(
            force=False,
            now_utc=now_utc,
            last_successful_cursor=last_successful_cursor,
            pending_range_start_at=None,
            pending_range_end_at=None,
            next_device_offset=0,
            next_retry_at=None,
            overlap=timedelta(minutes=10),
            stale_after=timedelta(minutes=90),
            bootstrap_span=timedelta(hours=6),
            effective_window=timedelta(minutes=20),
        )

        self.assertEqual(
            plan,
            CatchupPlan(
                start_at=datetime(2026, 8, 15, 23, 50, tzinfo=self.utc),
                end_at=datetime(2026, 8, 16, 0, 10, tzinfo=self.utc),
                offset=0,
            ),
        )

    def test_pending_range_resume_preserves_offset(self) -> None:
        now_utc = datetime(2026, 8, 16, 3, 15, tzinfo=self.utc)
        pending_start = datetime(2026, 8, 16, 3, 0, tzinfo=self.utc)
        pending_end = datetime(2026, 8, 16, 3, 25, tzinfo=self.utc)

        plan = self.service._plan_operational_catchup(
            force=False,
            now_utc=now_utc,
            last_successful_cursor=datetime(2026, 8, 16, 2, 50, tzinfo=self.utc),
            pending_range_start_at=pending_start,
            pending_range_end_at=pending_end,
            next_device_offset=17,
            next_retry_at=None,
            overlap=timedelta(minutes=10),
            stale_after=timedelta(minutes=90),
            bootstrap_span=timedelta(hours=6),
            effective_window=timedelta(minutes=20),
        )

        self.assertEqual(
            plan,
            CatchupPlan(
                start_at=pending_start,
                end_at=datetime(2026, 8, 16, 3, 20, tzinfo=self.utc),
                offset=17,
            ),
        )

    def test_retry_window_blocks_until_retry_time(self) -> None:
        now_utc = datetime(2026, 8, 16, 3, 15, tzinfo=self.utc)

        plan = self.service._plan_operational_catchup(
            force=False,
            now_utc=now_utc,
            last_successful_cursor=datetime(2026, 8, 16, 3, 5, tzinfo=self.utc),
            pending_range_start_at=None,
            pending_range_end_at=None,
            next_device_offset=0,
            next_retry_at=datetime(2026, 8, 16, 3, 20, tzinfo=self.utc),
            overlap=timedelta(minutes=10),
            stale_after=timedelta(minutes=90),
            bootstrap_span=timedelta(hours=6),
            effective_window=timedelta(minutes=20),
        )

        self.assertIsNone(plan)


if __name__ == "__main__":
    unittest.main()
