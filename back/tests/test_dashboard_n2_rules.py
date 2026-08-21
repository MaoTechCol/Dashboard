from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.database import Base
from app.core.settings import Settings
from app.models import AlarmEvent, ReconciliationReview
from app.schemas import CompanyBrand, CompanyConfig, DashboardRules
from app.services.company_registry import CompanyRegistry
from app.services.dashboard import DashboardService, _build_recent_episode_analysis


TZ = ZoneInfo("America/Bogota")


def company() -> CompanyConfig:
    return CompanyConfig(
        slug="ismocol",
        name="ISMOCOL",
        customer="ISMOCOL",
        timezone="America/Bogota",
        brand=CompanyBrand(eyebrow="DMS", title="DMS", subtitle="DMS"),
        rules=DashboardRules(),
    )


def event(guid: str, category: str, local_at: datetime, *, plate: str = "ABC123") -> AlarmEvent:
    occurred_at = local_at.replace(tzinfo=TZ).astimezone(ZoneInfo("UTC"))
    return AlarmEvent(
        guid=guid,
        device_id=f"device-{plate}",
        plate_no=plate,
        company_slug="ismocol",
        category=category,
        classification_status="classified_dms",
        occurred_at=occurred_at,
        received_at=occurred_at,
        source="harvest",
    )


def analyze(
    events: list[AlarmEvent],
    *,
    reviews: dict[str, str] | None = None,
    fleet_vehicle_count: int = 10,
) -> dict:
    return _build_recent_episode_analysis(
        events,
        company(),
        TZ,
        {},
        review_status_by_guid=reviews,
        fleet_vehicle_count=fleet_vehicle_count,
    )


class DashboardN2RuleTests(unittest.TestCase):
    def test_phone_is_critical_from_one_event(self) -> None:
        result = analyze([event("phone-1", "Uso de celular", datetime(2026, 8, 20, 8, 0))])
        self.assertEqual(result["episodes"][0]["level"], "critico")

    def test_eye_closed_is_high_from_one_and_critical_from_two(self) -> None:
        one = analyze([event("eye-1", "Ojos cerrados", datetime(2026, 8, 20, 6, 0))])
        two = analyze(
            [
                event("eye-1", "Ojos cerrados", datetime(2026, 8, 20, 6, 0)),
                event("eye-2", "Ojos cerrados", datetime(2026, 8, 20, 6, 5)),
            ]
        )
        self.assertEqual(one["episodes"][0]["level"], "alto")
        self.assertEqual(two["episodes"][0]["level"], "critico")

    def test_sixth_daytime_eye_closed_requires_review_and_approval_overrides(self) -> None:
        start = datetime(2026, 8, 20, 7, 10)
        events = [event(f"eye-{index}", "Ojos cerrados", start + timedelta(minutes=index * 20)) for index in range(6)]
        pending = analyze(events)
        approved = analyze(events, reviews={"eye-5": "approved"})
        self.assertEqual(pending["guid_status"]["eye-5"]["reason"], "eyes_closed_daytime_limit")
        self.assertEqual(pending["metrics"]["suppressed_by_rule"], 1)
        self.assertIn(approved["guid_status"]["eye-5"]["visibility_status"], {"visible_episode", "fused_in_episode"})

    def test_collision_and_yawning_thresholds(self) -> None:
        start = datetime(2026, 8, 20, 18, 0)
        collisions = [event(f"collision-{index}", "Riesgo de colision", start + timedelta(minutes=index * 5)) for index in range(3)]
        yawns = [event(f"yawn-{index}", "Bostezo", start + timedelta(minutes=index * 5), plate="DEF456") for index in range(4)]
        result = analyze(collisions + yawns)
        by_category = {row["category"]: row for row in result["episodes"]}
        self.assertEqual(by_category["Riesgo de colision"]["level"], "alto")
        self.assertEqual(by_category["Bostezo"]["level"], "alto")

    def test_camera_requires_consecutive_days_for_high(self) -> None:
        result = analyze(
            [
                event("camera-1", "Camara cubierta", datetime(2026, 8, 19, 12, 0)),
                event("camera-2", "Camara cubierta", datetime(2026, 8, 20, 12, 0)),
            ]
        )
        self.assertEqual([row["level"] for row in result["episodes"]], ["medio", "alto"])

    def test_smoking_is_grouped_by_operational_shift(self) -> None:
        result = analyze(
            [
                event("smoke-1", "Fumando", datetime(2026, 8, 20, 8, 0)),
                event("smoke-2", "Fumando", datetime(2026, 8, 20, 16, 0)),
            ]
        )
        self.assertEqual(len(result["episodes"]), 1)
        self.assertEqual(result["episodes"][0]["guid_count"], 2)

    def test_distraction_uses_three_times_fleet_average(self) -> None:
        start = datetime(2026, 8, 20, 18, 0)
        events = [
            *[event(f"low-{index}", "Distraccion", start + timedelta(minutes=index * 20), plate="LOW123") for index in range(3)],
            *[event(f"high-{index}", "Distraccion", start + timedelta(minutes=index * 20), plate="HIG456") for index in range(7)],
        ]
        result = analyze(events, fleet_vehicle_count=10)
        self.assertTrue(all(result["guid_status"][f"low-{index}"]["visibility_status"] == "suppressed_by_rule" for index in range(3)))
        self.assertTrue(all(result["guid_status"][f"high-{index}"]["visibility_status"] in {"visible_episode", "fused_in_episode"} for index in range(7)))

    def test_yawn_then_eye_closed_becomes_one_fatigue_episode(self) -> None:
        result = analyze(
            [
                event("yawn-1", "Bostezo", datetime(2026, 8, 20, 5, 0)),
                event("eye-1", "Ojos cerrados", datetime(2026, 8, 20, 5, 20)),
            ]
        )
        self.assertEqual(len(result["episodes"]), 1)
        self.assertEqual(result["episodes"][0]["category"], "Fatiga en progresion")
        self.assertEqual(result["episodes"][0]["guid_count"], 2)

    def test_materialized_snapshot_persists_suppressed_review_after_session_closes(self) -> None:
        engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            future=True,
        )
        Base.metadata.create_all(engine)
        sessions = sessionmaker(bind=engine, autoflush=False, expire_on_commit=True, future=True)
        reference_at = datetime(2026, 8, 20, 18, 0, tzinfo=timezone.utc)

        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "companies.json"
            config_path.write_text(
                json.dumps([company().model_dump(mode="json")]),
                encoding="utf-8",
            )
            registry = CompanyRegistry(config_path)
            service = DashboardService(
                session_factory=sessions,
                registry=registry,
                settings=Settings(_env_file=None),
            )
            with sessions() as session:
                session.add(
                    AlarmEvent(
                        guid="detached-review",
                        provider_event_key="provider-detached-review",
                        device_id="device-1",
                        plate_no="ABC123",
                        company_slug="ismocol",
                        category="Distraccion",
                        subtype="Distraccion",
                        classification_status="classified_dms",
                        raw_alarm_type="Distracted Driving",
                        raw_tp="65",
                        raw_event_code="130",
                        raw_event_time="2026-08-20 13:00:00",
                        occurred_at=reference_at - timedelta(hours=1),
                        received_at=reference_at - timedelta(hours=1),
                        source="harvest",
                    )
                )
                session.commit()

            service.build_snapshot(
                "ismocol",
                force_recompute=True,
                published_cut_at=reference_at,
            )

            with sessions() as session:
                review = session.scalar(
                    select(ReconciliationReview).where(
                        ReconciliationReview.review_key == "suppressed:detached-review"
                    )
                )
                self.assertIsNotNone(review)
                self.assertEqual(review.reason, "distraction_below_3x_fleet_average")
                self.assertIn("provider-detached-review", review.portal_payload_json)


if __name__ == "__main__":
    unittest.main()
