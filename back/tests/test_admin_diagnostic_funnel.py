from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.database import Base
from app.core.settings import Settings
from app.models import AlarmEvent, HowenAlarmRaw, PublishedDashboardSnapshot, ReconciliationReview
from app.schemas import CompanyBrand, CompanyConfig, DashboardRules
from app.services.company_registry import CompanyRegistry
from app.services.dashboard import DashboardService


def _company() -> CompanyConfig:
    return CompanyConfig(
        slug="ismocol",
        name="ISMOCOL",
        customer="ISMOCOL",
        timezone="America/Bogota",
        brand=CompanyBrand(eyebrow="DMS", title="DMS", subtitle="DMS"),
        rules=DashboardRules(),
    )


class AdminDiagnosticFunnelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        config_path = Path(self.temp_dir.name) / "companies.json"
        config_path.write_text(json.dumps([_company().model_dump(mode="json")]), encoding="utf-8")
        engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            future=True,
        )
        Base.metadata.create_all(engine)
        self.sessions = sessionmaker(bind=engine, autoflush=False, expire_on_commit=True, future=True)
        self.service = DashboardService(
            session_factory=self.sessions,
            registry=CompanyRegistry(config_path),
            settings=Settings(_env_file=None),
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def _raw(guid: str, occurred_at: datetime, *, received_at: datetime, temporal: str = "accepted") -> HowenAlarmRaw:
        return HowenAlarmRaw(
            guid=guid,
            provider_event_key=f"provider-{guid}",
            company_slug="ismocol",
            device_id="device-1",
            plate_no="ABC123",
            source="harvest",
            occurred_at=occurred_at,
            received_at=received_at,
            raw_alarm_type="Using Phone While Driving",
            classification_status="classified_dms",
            mapped_category="Uso de celular",
            temporal_status=temporal,
            ingest_result="inserted_alarm_event" if temporal == "accepted" else "future_rejected",
            payload_json="{}",
        )

    @staticmethod
    def _event(guid: str, occurred_at: datetime) -> AlarmEvent:
        return AlarmEvent(
            guid=guid,
            provider_event_key=f"provider-{guid}",
            company_slug="ismocol",
            device_id="device-1",
            plate_no="ABC123",
            category="Uso de celular",
            classification_status="classified_dms",
            occurred_at=occurred_at,
            received_at=occurred_at,
            source="harvest",
        )

    @staticmethod
    def _review(
        key: str,
        guid: str,
        observed_at: datetime,
        *,
        created_at: datetime,
        action: str,
        status: str,
        reason: str = "manual_review",
    ) -> ReconciliationReview:
        return ReconciliationReview(
            review_key=key,
            company_slug="ismocol",
            guid=guid,
            device_id="device-1",
            plate_no="ABC123",
            observed_at=observed_at,
            classification_status="classified_dms",
            visibility_status="pending_review",
            category="Uso de celular",
            reason=reason,
            suggested_action=action,
            review_status=status,
            portal_payload_json="{}",
            created_at=created_at,
        )

    def test_funnel_uses_event_time_and_exposes_unexplained_difference(self) -> None:
        end_at = datetime(2026, 8, 21, 18, 0, tzinfo=timezone.utc)
        start_at = end_at - timedelta(hours=24)
        current_at = end_at - timedelta(hours=1)
        historical_at = end_at - timedelta(days=3)
        with self.sessions() as session:
            session.add_all(
                [
                    self._raw("visible", current_at, received_at=current_at),
                    self._event("visible", current_at),
                    self._raw("retained", current_at, received_at=current_at),
                    self._review(
                        "retained",
                        "retained",
                        current_at,
                        created_at=current_at,
                        action="review_raw",
                        status="pending",
                    ),
                    self._raw("discarded", current_at, received_at=current_at),
                    self._event("discarded", current_at),
                    self._review(
                        "discarded",
                        "discarded",
                        current_at,
                        created_at=current_at,
                        action="review_visibility",
                        status="discarded",
                    ),
                    self._raw("future", current_at, received_at=current_at, temporal="future_rejected"),
                    self._raw("unexplained", current_at, received_at=current_at),
                    # This backfilled event was received now, but occurred outside the active window.
                    self._raw("historical", historical_at, received_at=current_at),
                    self._review(
                        "historical-review",
                        "historical",
                        historical_at,
                        created_at=current_at,
                        action="review_raw",
                        status="pending",
                    ),
                ]
            )
            session.commit()

        payload = self.service.build_admin_audit("ismocol", start_at, end_at)
        metrics = payload["requested_window"]
        self.assertEqual(metrics["received_dms"], 5)
        self.assertEqual(metrics["analytic_dms"], 2)
        self.assertEqual(metrics["visible_episodes"], 1)
        self.assertEqual(metrics["retained_for_review"], 1)
        self.assertEqual(metrics["discarded_by_admin"], 1)
        self.assertEqual(metrics["reconciled_dms"], 4)
        self.assertEqual(metrics["unexplained_difference"], 1)

    def test_review_queue_is_filtered_and_paginated_in_database(self) -> None:
        end_at = datetime(2026, 8, 21, 18, 0, tzinfo=timezone.utc)
        start_at = end_at - timedelta(hours=24)
        with self.sessions() as session:
            for index in range(30):
                action = "review_raw" if index % 2 else "review_visibility"
                reason = "mapping" if index % 3 else "rule"
                session.add(
                    self._review(
                        f"review-{index}",
                        f"guid-{index}",
                        start_at + timedelta(minutes=index),
                        created_at=end_at,
                        action=action,
                        status="pending",
                        reason=reason,
                    )
                )
            session.add(
                self._review(
                    "old-created-now",
                    "old-created-now",
                    start_at - timedelta(days=2),
                    created_at=end_at,
                    action="review_raw",
                    status="pending",
                )
            )
            session.commit()

        page = self.service.list_reconciliation_reviews(
            company_slug="ismocol",
            start_at=start_at,
            end_at=end_at,
            page=2,
            page_size=5,
            suggested_actions=["review_raw"],
            reasons=["mapping"],
        )
        self.assertEqual(page["total_items"], 30)
        self.assertEqual(page["filtered_items"], 10)
        self.assertEqual(page["page"], 2)
        self.assertEqual(page["total_pages"], 2)
        self.assertEqual(len(page["items"]), 5)
        self.assertTrue(all(row["suggested_action"] == "review_raw" for row in page["items"]))
        self.assertTrue(all(row["reason"] == "mapping" for row in page["items"]))

    def test_operational_recency_uses_the_last_published_visible_dms(self) -> None:
        published_at = datetime(2026, 8, 21, 17, 45, tzinfo=timezone.utc)
        with self.sessions() as session:
            session.add(
                PublishedDashboardSnapshot(
                    company_slug="ismocol",
                    cut_status="succeeded",
                    published_cut_at=published_at,
                    last_dms_published_at=published_at,
                    snapshot_json="{}",
                )
            )
            session.commit()

        with self.sessions() as session:
            recency = self.service._build_operational_recency(
                session,
                company=_company(),
                reference_at=published_at + timedelta(minutes=10),
            )

        self.assertEqual(recency["last_visible_dms_at"], "2026-08-21T17:45:00Z")


if __name__ == "__main__":
    unittest.main()
