from __future__ import annotations

import asyncio
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
        "howen_evidence_max_pages_per_batch": 100,
        "howen_evidence_max_devices_per_request": 2,
        "howen_request_spacing_seconds": 0,
        "howen_request_spacing_max_seconds": 0,
        "howen_request_recovery_successes": 20,
        "backfill_rate_limit_max_retries": 2,
        "backfill_rate_limit_cooldown_seconds": 0,
        "backfill_rate_limit_max_cooldown_seconds": 0,
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
                {"status": 10000, "data": {"list": []}},
                {"status": 10000, "data": {"list": [{"alarmGuid": "d"}]}},
                {"status": 10000, "data": {"list": []}},
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
        self.assertEqual(calls[2].args[1]["pageNum"], "3")
        self.assertEqual(calls[3].args[1]["conditionName"], "3")
        self.assertEqual(calls[4].args[1]["pageNum"], "2")

    async def test_continues_when_provider_caps_pages_below_requested_size(self) -> None:
        client = HowenClient(
            settings=_settings(howen_evidence_page_size=500),
            registry=_Registry(),
        )
        first_page = [{"alarmGuid": f"event-{index}"} for index in range(100)]
        client._post_form = AsyncMock(
            side_effect=[
                {"status": 10000, "data": {"list": first_page}},
                {"status": 10000, "data": {"list": [{"alarmGuid": "event-100"}]}},
                {"status": 10000, "data": {"list": []}},
            ]
        )

        rows = await client.fetch_evidence_alarms(
            "token",
            device_ids=["1"],
            start_at=datetime(2026, 8, 21, 10, 0),
            end_at=datetime(2026, 8, 21, 10, 30),
        )

        self.assertEqual(len(rows), 101)
        self.assertEqual(rows[-1]["alarmGuid"], "event-100")
        self.assertEqual(client._post_form.await_count, 3)

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

    async def test_retries_the_current_page_without_losing_previous_pages(self) -> None:
        client = HowenClient(settings=_settings(), registry=_Registry())
        client._post_form = AsyncMock(
            side_effect=[
                {"status": 10000, "data": {"list": [{"alarmGuid": "a"}, {"alarmGuid": "b"}]}},
                {"status": 10014, "msg": "Requests too frequent, please try again later"},
                {"status": 10000, "data": {"list": [{"alarmGuid": "c"}]}},
                {"status": 10000, "data": {"list": []}},
            ]
        )

        rows = await client.fetch_evidence_alarms(
            "token",
            device_ids=["1"],
            start_at=datetime(2026, 8, 21, 10, 0),
            end_at=datetime(2026, 8, 21, 10, 30),
        )

        self.assertEqual([row["alarmGuid"] for row in rows], ["a", "b", "c"])
        calls = client._post_form.await_args_list
        self.assertEqual([call.args[1]["pageNum"] for call in calls], ["1", "2", "2", "3"])

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
    def _service(self) -> IngestionService:
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
        companies = [
            SimpleNamespace(slug="alpha"),
            SimpleNamespace(slug="beta"),
        ]
        service.registry = SimpleNamespace(
            all=lambda: companies,
            is_operational=lambda company: True,
        )
        service._list_company_device_ids = lambda slug: ["1"] if slug == "alpha" else ["2"]
        service._evidence_fetch_tasks = {}
        service._historical_window_for_device = lambda **kwargs: (
            kwargs["start_at"],
            kwargs["end_at"],
        )
        service._record_normalization_failure = AsyncMock()
        return service

    async def test_partitions_company_result_by_device(self) -> None:
        service = self._service()

        grouped = await service._fetch_evidence_harvest_rows(
            device_ids=["1", "2"],
            start_at=datetime(2026, 8, 21, 10, 0, tzinfo=ZoneInfo("UTC")),
            end_at=datetime(2026, 8, 21, 10, 30, tzinfo=ZoneInfo("UTC")),
        )

        self.assertEqual([row["alarmGuid"] for row in grouped["1"]], ["a", "c"])
        self.assertEqual([row["alarmGuid"] for row in grouped["2"]], ["b"])

    async def test_companies_share_one_account_fetch_for_the_same_cut(self) -> None:
        service = self._service()
        start_at = datetime(2026, 8, 21, 10, 0, tzinfo=ZoneInfo("UTC"))
        end_at = datetime(2026, 8, 21, 10, 30, tzinfo=ZoneInfo("UTC"))

        alpha, beta = await asyncio.gather(
            service._fetch_evidence_harvest_rows(
                device_ids=["1"],
                start_at=start_at,
                end_at=end_at,
            ),
            service._fetch_evidence_harvest_rows(
                device_ids=["2"],
                start_at=start_at,
                end_at=end_at,
            ),
        )

        self.assertEqual(service.howen.fetch_evidence_alarms_authorized.await_count, 1)
        self.assertEqual([row["alarmGuid"] for row in alpha["1"]], ["a", "c"])
        self.assertEqual([row["alarmGuid"] for row in beta["2"]], ["b"])

    async def test_historical_rebuild_fetches_only_requested_company_devices(self) -> None:
        service = self._service()

        grouped = await service._fetch_evidence_harvest_rows(
            device_ids=["1"],
            start_at=datetime(2026, 8, 1, 0, 0, tzinfo=ZoneInfo("UTC")),
            end_at=datetime(2026, 8, 21, 23, 59, tzinfo=ZoneInfo("UTC")),
            account_scope=False,
        )

        request = service.howen.fetch_evidence_alarms_authorized.await_args.kwargs
        self.assertEqual(request["device_ids"], ["1"])
        self.assertEqual([row["alarmGuid"] for row in grouped["1"]], ["a", "c"])

    async def test_backfill_splits_wide_ranges_into_daily_provider_windows(self) -> None:
        service = self._service()
        service.settings = _settings(
            howen_evidence_max_range_days=1,
            backfill_rate_limit_max_retries=0,
            backfill_rate_limit_cooldown_seconds=0,
            backfill_rate_limit_max_cooldown_seconds=0,
        )
        service.howen.is_rate_limited = lambda exc: False
        service._fetch_evidence_harvest_rows = AsyncMock(
            side_effect=[
                {"1": [{"alarmGuid": "day-1", "deviceID": "1"}]},
                {"1": [{"alarmGuid": "day-2", "deviceID": "1"}]},
                {"1": [{"alarmGuid": "day-3", "deviceID": "1"}]},
            ]
        )

        start_at = datetime(2026, 7, 24, 0, 0, tzinfo=ZoneInfo("UTC"))
        end_at = datetime(2026, 7, 26, 23, 59, 59, tzinfo=ZoneInfo("UTC"))
        grouped = await service._fetch_evidence_backfill_rows(
            device_ids=["1"],
            start_at=start_at,
            end_at=end_at,
            source="harvest",
            company_slug="alpha",
            defer_on_rate_limit=False,
        )

        self.assertEqual(
            [row["alarmGuid"] for row in grouped["1"]],
            ["day-1", "day-2", "day-3"],
        )
        calls = service._fetch_evidence_harvest_rows.await_args_list
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[0].kwargs["start_at"], start_at)
        self.assertEqual(
            calls[0].kwargs["end_at"],
            datetime(2026, 7, 24, 23, 59, 59, tzinfo=ZoneInfo("UTC")),
        )
        self.assertEqual(
            calls[-1].kwargs["end_at"],
            end_at,
        )


if __name__ == "__main__":
    unittest.main()
