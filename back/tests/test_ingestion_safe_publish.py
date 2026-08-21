from __future__ import annotations

import asyncio
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch
import unittest
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.database import Base
from app.models import AlarmHarvestRun, PublishedDashboardSnapshot
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


class _CohortRegistry:
    def __init__(self) -> None:
        self.companies = [
            SimpleNamespace(slug="alpha", timezone="America/Bogota", device_ids=["a"], fleet_ids=[]),
            SimpleNamespace(slug="beta", timezone="America/Bogota", device_ids=["b"], fleet_ids=[]),
        ]

    def reload(self) -> None:
        return None

    def all(self):
        return self.companies

    @staticmethod
    def is_operational(company) -> bool:
        return bool(company.device_ids or company.fleet_ids)


class _CohortHub:
    def __init__(self) -> None:
        self.published: list[str] = []

    async def publish(self, company_slug: str, payload: dict) -> None:
        self.published.append(company_slug)


class CohortPublicationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            future=True,
        )
        Base.metadata.create_all(engine)
        self.session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=True, future=True)
        self.cut_at = datetime(2026, 8, 21, 3, 15, tzinfo=ZoneInfo("UTC"))
        self.service = IngestionService.__new__(IngestionService)
        self.service.session_factory = self.session_factory
        self.service.registry = _CohortRegistry()
        self.service._cut_publish_locks = {}
        self.service._dirty = asyncio.Event()
        self.service.hub = _CohortHub()

        service = self.service

        class Dashboard:
            def clear_runtime_caches(self) -> None:
                return None

            def materialize_snapshot(self, company_slug: str, *, cut_at: datetime, cut_status: str):
                with service.session_factory() as session:
                    publication = session.get(PublishedDashboardSnapshot, company_slug)
                    if publication is None:
                        publication = PublishedDashboardSnapshot(company_slug=company_slug)
                    publication.published_cut_at = cut_at
                    publication.cut_status = cut_status
                    session.add(publication)
                    session.commit()
                return {"meta": {"publishedCutAt": cut_at.isoformat()}}

        self.service.dashboard = Dashboard()

    def _add_successful_run(self, company_slug: str) -> None:
        with self.session_factory() as session:
            session.add(
                AlarmHarvestRun(
                    company_slug=company_slug,
                    cut_at=self.cut_at,
                    window_start=self.cut_at,
                    window_end=self.cut_at,
                    status="succeeded",
                )
            )
            session.commit()

    async def test_cohort_waits_for_every_operational_company(self) -> None:
        self._add_successful_run("alpha")

        result = await self.service._publish_harvest_cohort_if_ready(cut_at=self.cut_at)

        self.assertFalse(result["cohort_ready"])
        self.assertEqual(result["cohort_waiting_for"], ["beta"])
        self.assertEqual(self.service.hub.published, [])

    async def test_last_company_publishes_the_same_cut_for_the_whole_cohort(self) -> None:
        self._add_successful_run("alpha")
        self._add_successful_run("beta")

        result = await self.service._publish_harvest_cohort_if_ready(cut_at=self.cut_at)

        self.assertTrue(result["cohort_ready"])
        self.assertEqual(result["cohort_published_companies"], ["alpha", "beta"])
        with self.session_factory() as session:
            alpha = session.get(PublishedDashboardSnapshot, "alpha")
            beta = session.get(PublishedDashboardSnapshot, "beta")
            self.assertEqual(alpha.published_cut_at, beta.published_cut_at)

if __name__ == "__main__":
    unittest.main()
