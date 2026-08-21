from __future__ import annotations

from datetime import datetime, timedelta
import unittest
from zoneinfo import ZoneInfo

from app.models import AlarmEvent
from app.schemas import CompanyBrand, CompanyConfig, DashboardRules
from app.services.dashboard import _build_recent_episode_analysis


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


if __name__ == "__main__":
    unittest.main()
