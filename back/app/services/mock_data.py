from __future__ import annotations

import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from app.core.catalog import CATEGORY_ORDER
from app.core.time import utc_now
from app.models import AlarmEvent, DailyMileageSnapshot, DeviceRecord, MileageReading
from app.schemas import NormalizedAlarm, NormalizedStatus


class MockDataService:
    def __init__(self, *, session_factory, registry, settings) -> None:
        self.session_factory = session_factory
        self.registry = registry
        self.settings = settings
        self.random = random.Random(42)

    def seed_if_empty(self) -> None:
        with self.session_factory() as session:
            if session.scalar(select(func.count()).select_from(AlarmEvent)):
                return

            tz = ZoneInfo(self.registry.all()[0].timezone if self.registry.all() else self.settings.default_timezone)
            now_local = utc_now().astimezone(tz)
            plates = [
                "NXR233",
                "NXR223",
                "GHO280",
                "GHO069",
                "GHO191",
                "NXR225",
                "NXR221",
                "WOM820",
                "NXR239",
                "WOM819",
                "LPK636",
            ] + [f"DSM{index:03d}" for index in range(1, 28)]

            cumulative_km: dict[str, float] = defaultdict(float)
            for index, plate in enumerate(plates):
                session.add(
                    DeviceRecord(
                        device_id=f"DEV-{index + 1:04d}",
                        plate_no=plate,
                        fleet_id="cotaba-main",
                        device_name=plate,
                    )
                )

            for offset in range(35, -1, -1):
                current_day = now_local.date() - timedelta(days=offset)
                for index, plate in enumerate(plates):
                    if self.random.random() < 0.14:
                        continue
                    day_km = round(self.random.uniform(22, 128), 1)
                    cumulative_km[plate] += day_km
                    recorded_local = datetime.combine(current_day, datetime.min.time(), tzinfo=tz) + timedelta(hours=18)
                    session.add(
                        MileageReading(
                            device_id=f"DEV-{index + 1:04d}",
                            plate_no=plate,
                            fleet_id="cotaba-main",
                            recorded_at=recorded_local.astimezone(timezone.utc),
                            total_km=round(cumulative_km[plate], 1),
                            day_km=day_km,
                            source="mock-seed",
                        )
                    )
                    session.add(
                        DailyMileageSnapshot(
                            device_id=f"DEV-{index + 1:04d}",
                            plate_no=plate,
                            fleet_id="cotaba-main",
                            snapshot_date=current_day,
                            observed_at=recorded_local.astimezone(timezone.utc),
                            total_km=round(cumulative_km[plate], 1),
                            day_km=day_km,
                        )
                    )

                    event_count = self.random.randint(0, 5)
                    for _ in range(event_count):
                        category = self.random.choices(
                            CATEGORY_ORDER,
                            weights=[2, 1, 6, 2, 3, 1, 1, 2],
                            k=1,
                        )[0]
                        hour = self.random.randint(0, 23)
                        minute = self.random.randint(0, 59)
                        occurred_local = datetime.combine(current_day, datetime.min.time(), tzinfo=tz) + timedelta(
                            hours=hour,
                            minutes=minute,
                        )
                        session.add(
                            AlarmEvent(
                                guid=uuid4().hex,
                                device_id=f"DEV-{index + 1:04d}",
                                plate_no=plate,
                                fleet_id="cotaba-main",
                                category=category,
                                subtype=category,
                                occurred_at=occurred_local.astimezone(timezone.utc),
                                start_at=occurred_local.astimezone(timezone.utc),
                                end_at=(occurred_local + timedelta(minutes=1)).astimezone(timezone.utc),
                                total_mileage_km=round(cumulative_km[plate], 1),
                                raw_payload="{}",
                            )
                        )

            base_today = datetime.combine(now_local.date(), datetime.min.time(), tzinfo=tz)
            scenario = [
                ("NXR233", "Ojos cerrados", 747),
                ("NXR233", "Ojos cerrados", 751),
                ("NXR233", "Ojos cerrados", 754),
                ("NXR233", "Ojos cerrados", 757),
                ("NXR233", "Ojos cerrados", 759),
                ("NXR233", "Ojos cerrados", 761),
                ("GHO069", "Uso de celular", 756),
                ("GHO191", "Riesgo de colision", 685),
                ("GHO191", "Riesgo de colision", 700),
                ("GHO191", "Riesgo de colision", 718),
                ("GHO191", "Bostezo", 635),
                ("GHO191", "Bostezo", 648),
                ("GHO191", "Ojos cerrados", 665),
                ("GHO191", "Ojos cerrados", 672),
                ("GHO191", "Ojos cerrados", 680),
                ("GHO069", "Ojos cerrados", 665),
                ("GHO069", "Ojos cerrados", 671),
                ("GHO069", "Ojos cerrados", 678),
                ("NXR223", "Ojos cerrados", 640),
                ("NXR223", "Ojos cerrados", 645),
                ("NXR223", "Ojos cerrados", 652),
                ("GHO069", "Fumando", 570),
                ("NXR221", "Ojos cerrados", 552),
                ("NXR221", "Ojos cerrados", 560),
                ("GHO069", "Uso de celular", 472),
                ("GHO280", "Uso de celular", 382),
                ("GHO280", "Ojos cerrados", 250),
                ("GHO280", "Ojos cerrados", 256),
                ("GHO280", "Ojos cerrados", 262),
                ("NXR225", "Bostezo", 190),
                ("NXR225", "Bostezo", 202),
                ("NXR225", "Bostezo", 215),
                ("NXR225", "Bostezo", 224),
                ("NXR225", "Bostezo", 235),
                ("NXR225", "Bostezo", -80),
                ("NXR225", "Bostezo", -68),
                ("NXR225", "Bostezo", -55),
                ("WOM820", "Ojos cerrados", 485),
                ("NXR239", "Ojos cerrados", 588),
                ("WOM819", "Ojos cerrados", 510),
                ("GHO280", "Ojos cerrados", 430),
                ("NXR221", "Ojos cerrados", 725),
                ("LPK636", "Distraccion", 600),
            ]
            for plate, category, minute_of_day in scenario:
                if minute_of_day >= 0:
                    occurred_local = base_today + timedelta(minutes=minute_of_day)
                else:
                    occurred_local = base_today - timedelta(days=1) + timedelta(minutes=24 * 60 + minute_of_day)
                session.add(
                    AlarmEvent(
                        guid=uuid4().hex,
                        device_id=f"DEV-{plates.index(plate) + 1:04d}",
                        plate_no=plate,
                        fleet_id="cotaba-main",
                        category=category,
                        subtype=category,
                        occurred_at=occurred_local.astimezone(timezone.utc),
                        start_at=occurred_local.astimezone(timezone.utc),
                        end_at=(occurred_local + timedelta(minutes=1)).astimezone(timezone.utc),
                        total_mileage_km=round(cumulative_km[plate], 1),
                        raw_payload="{}",
                    )
                )

            session.commit()

    def generate_tick(self) -> tuple[list[NormalizedStatus], list[NormalizedAlarm]]:
        now = utc_now()
        with self.session_factory() as session:
            devices = list(session.scalars(select(DeviceRecord).order_by(DeviceRecord.device_id)))

        chosen = self.random.sample(devices, k=min(4, len(devices)))
        statuses: list[NormalizedStatus] = []
        alarms: list[NormalizedAlarm] = []
        for device in chosen:
            km_delta = round(self.random.uniform(0.5, 3.8), 1)
            total_km = round((device.last_total_km or 0.0) + km_delta, 1)
            day_km = round((device.last_day_km or 0.0) + km_delta, 1)
            statuses.append(
                NormalizedStatus(
                    device_id=device.device_id,
                    observed_at=now,
                    total_km=total_km,
                    day_km=day_km,
                    plate_no=device.plate_no,
                    fleet_id=device.fleet_id,
                    driver_name=device.driver_name,
                    device_name=device.device_name,
                    raw={"source": "mock-tick"},
                )
            )
            if self.random.random() < 0.55:
                category = self.random.choices(
                    CATEGORY_ORDER,
                    weights=[2, 1, 4, 2, 2, 1, 1, 2],
                    k=1,
                )[0]
                alarms.append(
                    NormalizedAlarm(
                        guid=uuid4().hex,
                        device_id=device.device_id,
                        occurred_at=now,
                        category=category,
                        subtype=category,
                        plate_no=device.plate_no,
                        fleet_id=device.fleet_id,
                        driver_name=device.driver_name,
                        total_mileage_km=total_km,
                        raw={"source": "mock-tick"},
                    )
                )
        return statuses, alarms
