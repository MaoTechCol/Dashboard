from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch
import unittest
from zoneinfo import ZoneInfo

from app.models import PublishedDashboardSnapshot
from app.services.ingestion import IngestionService


class _DummySession:
    def __init__(self, publication: PublishedDashboardSnapshot | None) -> None:
        self.publication = publication

    def __enter__(self) -> _DummySession:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def get(self, model, key):
        if model is PublishedDashboardSnapshot and key == "ismocol":
            return self.publication
        return None


class _DummyRegistry:
    def get(self, slug: str):
        return SimpleNamespace(slug=slug, timezone="America/Bogota")


class IngestionSafePublishTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = IngestionService.__new__(IngestionService)
        self.service.registry = _DummyRegistry()
        self.service.settings = SimpleNamespace(
            default_timezone="UTC",
            harvest_cut_interval_minutes=15,
            harvest_overlap_minutes=30,
        )
        self.utc = ZoneInfo("UTC")

    def test_past_rebuild_range_keeps_current_publication_cut(self) -> None:
        current_cut = datetime(2026, 8, 16, 17, 45, tzinfo=self.utc)
        publication = PublishedDashboardSnapshot(company_slug="ismocol", published_cut_at=current_cut)
        self.service.session_factory = lambda: _DummySession(publication)
        self.service._latest_due_cut = lambda: current_cut

        with patch("app.services.ingestion.utc_now", return_value=datetime(2026, 8, 16, 18, 0, tzinfo=self.utc)):
            resolved_cut = self.service._resolve_safe_publish_cut_for_range(
                company_slug="ismocol",
                range_end_at=datetime(2026, 8, 14, 23, 59, 59, tzinfo=ZoneInfo("America/Bogota")),
            )

        self.assertEqual(resolved_cut, current_cut)

    def test_old_harvest_cut_never_rolls_publication_backward(self) -> None:
        current_cut = datetime(2026, 8, 16, 17, 45, tzinfo=self.utc)
        publication = PublishedDashboardSnapshot(company_slug="ismocol", published_cut_at=current_cut)
        self.service.session_factory = lambda: _DummySession(publication)

        resolved_cut = self.service._resolve_safe_publish_cut_for_harvest(
            company_slug="ismocol",
            harvested_cut_at=datetime(2026, 8, 16, 16, 15, tzinfo=self.utc),
        )

        self.assertEqual(resolved_cut, current_cut)

    def test_old_refresh_cut_is_superseded_by_current_publication(self) -> None:
        current_cut = datetime(2026, 8, 16, 17, 45, tzinfo=self.utc)
        publication = PublishedDashboardSnapshot(company_slug="ismocol", published_cut_at=current_cut)
        self.service.session_factory = lambda: _DummySession(publication)

        self.assertTrue(
            self.service.is_cut_superseded(
                company_slug="ismocol",
                cut_at=datetime(2026, 8, 16, 17, 30, tzinfo=self.utc),
            )
        )
        self.assertFalse(
            self.service.is_cut_superseded(
                company_slug="ismocol",
                cut_at=current_cut,
            )
        )

    def test_harvest_window_absorbs_gap_from_last_publication(self) -> None:
        published_cut = datetime(2026, 8, 16, 12, 0, tzinfo=self.utc)
        cut_at = datetime(2026, 8, 16, 13, 0, tzinfo=self.utc)
        publication = PublishedDashboardSnapshot(company_slug="ismocol", published_cut_at=published_cut)
        self.service.session_factory = lambda: _DummySession(publication)

        window_start, window_end = self.service._harvest_window_for_cut(
            cut_at,
            company_slug="ismocol",
        )

        self.assertEqual(window_start, datetime(2026, 8, 16, 11, 30, tzinfo=self.utc))
        self.assertEqual(window_end, cut_at)

    def test_harvest_window_keeps_normal_overlap_without_gap(self) -> None:
        published_cut = datetime(2026, 8, 16, 12, 45, tzinfo=self.utc)
        cut_at = datetime(2026, 8, 16, 13, 0, tzinfo=self.utc)
        publication = PublishedDashboardSnapshot(company_slug="ismocol", published_cut_at=published_cut)
        self.service.session_factory = lambda: _DummySession(publication)

        window_start, window_end = self.service._harvest_window_for_cut(
            cut_at,
            company_slug="ismocol",
        )

        self.assertEqual(window_start, datetime(2026, 8, 16, 12, 15, tzinfo=self.utc))
        self.assertEqual(window_end, cut_at)


if __name__ == "__main__":
    unittest.main()
