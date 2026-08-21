from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
import unittest
from zoneinfo import ZoneInfo

from app.services.howen import HowenClient
from app.services.ingestion import IngestionService


class _Registry:
    @staticmethod
    def resolve_company(*, device_id: str, fleet_id: str | None):
        return SimpleNamespace(slug="ismocol")

    @staticmethod
    def normalize_plate(company, value: str | None) -> str | None:
        return value

    @staticmethod
    def normalize_plate_any(value: str | None) -> str | None:
        return value

    @staticmethod
    def timezone_for(**kwargs) -> str:
        return "America/Bogota"

    @staticmethod
    def subtype_map() -> dict[str, str]:
        return {}


def _settings(**overrides):
    values = {
        "howen_http_base": "https://provider.example/vss",
        "howen_username": "account",
        "howen_evidence_page_size": 2,
        "howen_evidence_max_devices_per_request": 2,
        "howen_request_spacing_seconds": 0,
        "howen_request_spacing_max_seconds": 0,
        "howen_request_recovery_successes": 20,
        "backfill_rate_limit_cooldown_seconds": 0,
        "default_timezone": "America/Bogota",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class HowenEvidenceBulkTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_every_page_and_chunks_device_selection(self) -> None:
        client = HowenClient(settings=_settings(), registry=_Registry())
        client._post_form = AsyncMock(
            side_effect=[
                {"status": 10000, "data": {"list": [{"alarmGuid": "a"}, {"alarmGuid": "b"}]}},
                {"status": 10000, "data": {"list": [{"alarmGuid": "c"}]}},
                {"status": 10000, "data": {"list": [{"alarmGuid": "d"}]}},
            ]
        )

        rows = await client.fetch_evidence_alarms(
            "token",
            device_ids=["3", "1", "2"],
            start_at=datetime(2026, 8, 21, 10, 0),
            end_at=datetime(2026, 8, 21, 10, 30),
        )

        self.assertEqual([row["alarmGuid"] for row in rows], ["a", "b", "c", "d"])
        calls = client._post_form.await_args_list
        self.assertEqual(calls[0].args[1]["conditionName"], "1,2")
        self.assertEqual(calls[0].args[1]["pageNum"], "1")
        self.assertEqual(calls[1].args[1]["pageNum"], "2")
        self.assertEqual(calls[2].args[1]["conditionName"], "3")

    async def test_stops_if_provider_repeats_the_same_page(self) -> None:
        client = HowenClient(settings=_settings(), registry=_Registry())
        repeated = {"status": 10000, "data": {"list": [{"alarmGuid": "a"}, {"alarmGuid": "b"}]}}
        client._post_form = AsyncMock(side_effect=[repeated, repeated])

        rows = await client.fetch_evidence_alarms(
            "token",
            device_ids=["1"],
            start_at=datetime(2026, 8, 21, 10, 0),
            end_at=datetime(2026, 8, 21, 10, 30),
        )

        self.assertEqual([row["alarmGuid"] for row in rows], ["a", "b"])
        self.assertEqual(client._post_form.await_count, 2)

    def test_normalizes_alarm_clip_fields_without_losing_portal_identity(self) -> None:
        client = HowenClient(settings=_settings(), registry=_Registry())

        alarm = client.normalize_alarm(
            {
                "alarmGuid": "provider-guid",
                "deviceID": "862708048919885",
                "deviceName": "GHO280",
                "alarmType": "110",
                "alarmTypeValue": "Forward Collision Warning",
                "alarmTime": "2026-08-21 12:27:37",
                "alarmTimeEnd": "2026-08-21 12:27:40",
                "alarmGps": "-74.503,6.082",
                "speed": 28.19,
            }
        )

        self.assertIsNotNone(alarm)
        assert alarm is not None
        self.assertEqual(alarm.guid, "provider-guid")
        self.assertEqual(alarm.plate_no, "GHO280")
        self.assertEqual(alarm.category, "Riesgo de colision")
        self.assertEqual(alarm.classification_status, "classified_dms")
        self.assertEqual(alarm.occurred_at, datetime(2026, 8, 21, 17, 27, 37, tzinfo=ZoneInfo("UTC")))
        self.assertEqual(alarm.end_at, datetime(2026, 8, 21, 17, 27, 40, tzinfo=ZoneInfo("UTC")))


class EvidencePartitionTests(unittest.IsolatedAsyncioTestCase):
    async def test_partitions_company_result_by_device(self) -> None:
        service = IngestionService.__new__(IngestionService)
        service.howen = SimpleNamespace(
            fetch_evidence_alarms_authorized=AsyncMock(
                return_value=[
                    {"alarmGuid": "a", "deviceID": "1"},
                    {"alarmGuid": "b", "deviceID": "2"},
                    {"alarmGuid": "c", "deviceID": "1"},
                ]
            )
        )
        service._historical_window_for_device = lambda **kwargs: (
            kwargs["start_at"],
            kwargs["end_at"],
        )
        service._record_normalization_failure = AsyncMock()

        grouped = await service._fetch_evidence_harvest_rows(
            device_ids=["1", "2"],
            start_at=datetime(2026, 8, 21, 10, 0, tzinfo=ZoneInfo("UTC")),
            end_at=datetime(2026, 8, 21, 10, 30, tzinfo=ZoneInfo("UTC")),
        )

        self.assertEqual([row["alarmGuid"] for row in grouped["1"]], ["a", "c"])
        self.assertEqual([row["alarmGuid"] for row in grouped["2"]], ["b"])


if __name__ == "__main__":
    unittest.main()
