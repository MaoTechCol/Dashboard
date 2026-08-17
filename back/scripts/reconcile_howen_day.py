from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
import sys
from zoneinfo import ZoneInfo

from sqlalchemy import select

from app.core.database import SessionLocal
from app.core.settings import get_settings
from app.models import DeviceRecord, HowenAlarmRaw
from app.services.company_registry import CompanyRegistry
from app.services.howen import HowenClient


@dataclass(frozen=True)
class ProviderEvent:
    device_id: str
    plate_no: str | None
    category: str
    raw_alarm_type: str | None
    occurred_at: datetime
    classification_status: str

    @property
    def match_key(self) -> tuple[str, str, str, str]:
        return (
            self.device_id,
            self.category,
            self.occurred_at.isoformat(),
            (self.raw_alarm_type or "").strip().lower(),
        )


@dataclass(frozen=True)
class LocalRawEvent:
    guid: str
    device_id: str | None
    plate_no: str | None
    category: str | None
    raw_alarm_type: str | None
    occurred_at: datetime | None
    classification_status: str | None

    @property
    def match_key(self) -> tuple[str, str, str, str] | None:
        if not self.device_id or not self.category or not self.occurred_at:
            return None
        return (
            self.device_id,
            self.category,
            self.occurred_at.isoformat(),
            (self.raw_alarm_type or "").strip().lower(),
        )


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--company", required=True)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD in company local time")
    parser.add_argument("--request-spacing-seconds", type=float, default=None)
    args = parser.parse_args()

    settings = get_settings()
    registry = CompanyRegistry(
        settings.company_config_path,
        seed_path=settings.company_seed_config_path,
        session_factory=SessionLocal,
    )
    company = registry.get(args.company)
    if args.request_spacing_seconds is not None:
        settings.howen_request_spacing_seconds = max(args.request_spacing_seconds, 0.0)
    tz = ZoneInfo(company.timezone or settings.default_timezone)
    utc = ZoneInfo("UTC")
    day_local = datetime.strptime(args.date, "%Y-%m-%d").date()
    start_local = datetime.combine(day_local, datetime.min.time(), tz)
    end_local = datetime.combine(day_local, datetime.max.time().replace(microsecond=0), tz)
    start_utc = start_local.astimezone(utc)
    end_utc = end_local.astimezone(utc)

    with SessionLocal() as session:
        devices = list(
            session.scalars(
                select(DeviceRecord)
                .where(DeviceRecord.company_slug == company.slug)
                .order_by(DeviceRecord.device_id)
            )
        )
        local_rows = list(
            session.scalars(
                select(HowenAlarmRaw)
                .where(
                    HowenAlarmRaw.company_slug == company.slug,
                    HowenAlarmRaw.occurred_at >= start_utc,
                    HowenAlarmRaw.occurred_at <= end_utc,
                )
                .order_by(HowenAlarmRaw.occurred_at)
            )
        )

    client = HowenClient(settings=settings, registry=registry)
    provider_rows: list[ProviderEvent] = []
    provider_total_rows = 0
    try:
        for index, device in enumerate(devices, start=1):
            rows = await client.fetch_historical_alarms_authorized(
                device_id=device.device_id,
                start_at=start_local,
                end_at=end_local,
                force_login=False,
            )
            provider_total_rows += len(rows)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                alarm = client.normalize_alarm(row)
                if not alarm:
                    continue
                provider_rows.append(
                    ProviderEvent(
                        device_id=alarm.device_id,
                        plate_no=alarm.plate_no,
                        category=alarm.category,
                        raw_alarm_type=alarm.raw_alarm_type,
                        occurred_at=alarm.occurred_at.astimezone(utc),
                        classification_status=alarm.classification_status,
                    )
                )
            progress = {
                "progress": f"{index}/{len(devices)}",
                "device_id": device.device_id,
                "plate_no": device.plate_no,
                "rows": len(rows),
                "provider_total_rows_so_far": provider_total_rows,
                "provider_dms_so_far": sum(1 for event in provider_rows if event.classification_status == "classified_dms"),
            }
            print(progress, flush=True)
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            await close()

    local_events = [
        LocalRawEvent(
            guid=row.guid,
            device_id=row.device_id,
            plate_no=row.plate_no,
            category=row.mapped_category,
            raw_alarm_type=row.raw_alarm_type,
            occurred_at=row.occurred_at.astimezone(utc) if row.occurred_at else None,
            classification_status=row.classification_status,
        )
        for row in local_rows
    ]

    provider_dms = [row for row in provider_rows if row.classification_status == "classified_dms"]
    local_dms = [row for row in local_events if row.classification_status == "classified_dms"]

    local_keys = {row.match_key for row in local_dms if row.match_key is not None}
    missing_provider_dms = [row for row in provider_dms if row.match_key not in local_keys]

    print(
        {
            "company": company.slug,
            "date_local": args.date,
            "provider_total_rows": provider_total_rows,
            "provider_normalized_rows": len(provider_rows),
            "provider_dms": len(provider_dms),
            "local_raw_total": len(local_rows),
            "local_raw_dms": len(local_dms),
            "missing_provider_dms": len(missing_provider_dms),
        }
    )
    sys.stdout.flush()
    print("PROVIDER_CLASS_BREAKDOWN", Counter(row.classification_status for row in provider_rows).most_common())
    print("LOCAL_CLASS_BREAKDOWN", Counter(row.classification_status for row in local_events).most_common())
    print("MISSING_PROVIDER_DMS_SAMPLES")
    for row in missing_provider_dms[:25]:
        print(
            {
                "device_id": row.device_id,
                "plate_no": row.plate_no,
                "category": row.category,
                "raw_alarm_type": row.raw_alarm_type,
                "occurred_at_utc": row.occurred_at.isoformat(),
                "occurred_at_local": row.occurred_at.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )


if __name__ == "__main__":
    asyncio.run(main())
