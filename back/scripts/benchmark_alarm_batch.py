from __future__ import annotations

import argparse
import asyncio
from datetime import timedelta
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker

from app.core.database import Base
from app.core.settings import get_settings
from app.core.time import utc_now
from app.models import AlarmEvent, AlarmEventAudit, HowenAlarmRaw
from app.schemas import NormalizedAlarm
from app.services.ingestion import IngestionService


class BenchmarkRegistry:
    def __init__(self) -> None:
        self.company = SimpleNamespace(slug="benchmark", timezone="America/Bogota")

    def resolve_company(self, **_kwargs):
        return self.company

    def normalize_plate_any(self, value):
        return str(value).upper() if value else None

    def normalize_plate(self, _company, value):
        return self.normalize_plate_any(value)

    def timezone_for(self, **_kwargs):
        return "America/Bogota"


class NoopDashboard:
    def clear_runtime_caches(self) -> None:
        return None


class NoopDirtyFlag:
    def set(self) -> None:
        return None


def build_alarm(index: int, occurred_at) -> NormalizedAlarm:
    category = "Ojos cerrados" if index % 4 else "Riesgo de colision"
    alarm_type = "Eyes Closed" if category == "Ojos cerrados" else "Forward Collision Warning"
    event_code = "104" if category == "Ojos cerrados" else "130"
    guid = f"benchmark-{index:07d}"
    return NormalizedAlarm(
        guid=guid,
        device_id="benchmark-device",
        occurred_at=occurred_at,
        category=category,
        raw_alarm_type=alarm_type,
        raw_event_code=event_code,
        mapping_source="text_alarm_type",
        classification_status="classified_dms",
        visibility_status="candidate",
        plate_no="ABC123",
        fleet_id="benchmark-fleet",
        raw_event_time=occurred_at.isoformat(),
        raw={
            "alarmID": guid,
            "deviceID": "benchmark-device",
            "plateNo": "ABC123",
            "alarmTypeValue": alarm_type,
            "alarmType": event_code,
            "reportTime": occurred_at.isoformat(),
        },
    )


async def run_benchmark(rows: int, batch_size: int, engine) -> dict[str, object]:
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
    service = IngestionService.__new__(IngestionService)
    service.settings = SimpleNamespace(
        anomaly_future_tolerance_minutes=5,
        default_timezone="America/Bogota",
        historical_batch_mode="activation_only",
        historical_batch_size=batch_size,
    )
    service.session_factory = sessions
    service.registry = BenchmarkRegistry()
    service.dashboard = NoopDashboard()
    service._dirty = NoopDirtyFlag()

    now = utc_now().replace(microsecond=0)
    alarms = [build_alarm(index, now - timedelta(seconds=index + 60)) for index in range(rows)]
    started = perf_counter()
    result = await service.ingest_alarm_batch(
        alarms,
        source="harvest",
        company=service.registry.company,
        batch_size=batch_size,
    )
    elapsed = perf_counter() - started
    with sessions() as session:
        counts = {
            "raw": session.scalar(select(func.count()).select_from(HowenAlarmRaw)),
            "dms": session.scalar(select(func.count()).select_from(AlarmEvent)),
            "audit": session.scalar(select(func.count()).select_from(AlarmEventAudit)),
        }
    return {
        "rows": rows,
        "batch_size": batch_size,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_second": round(rows / elapsed, 1),
        "transactions": result.chunks_committed,
        "counts": counts,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark the historical alarm batch pipeline")
    parser.add_argument("--rows", type=int, default=6000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--configured-postgres",
        action="store_true",
        help="Use DATABASE_URL in an isolated temporary PostgreSQL schema",
    )
    args = parser.parse_args()
    if args.configured_postgres:
        database_url = get_settings().database_url
        if not database_url.startswith("postgres"):
            raise RuntimeError("DATABASE_URL is not PostgreSQL")
        schema_name = f"dms_batch_benchmark_{uuid4().hex}"
        admin_engine = create_engine(database_url, future=True)
        with admin_engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
        benchmark_engine = create_engine(
            database_url,
            connect_args={"options": f"-csearch_path={schema_name}"},
            future=True,
        )
        try:
            result = asyncio.run(
                run_benchmark(
                    rows=max(args.rows, 1),
                    batch_size=max(args.batch_size, 1),
                    engine=benchmark_engine,
                )
            )
        finally:
            benchmark_engine.dispose()
            with admin_engine.begin() as connection:
                connection.execute(text(f'DROP SCHEMA "{schema_name}" CASCADE'))
            admin_engine.dispose()
    else:
        with TemporaryDirectory(prefix="dms-batch-") as directory:
            sqlite_engine = create_engine(f"sqlite:///{Path(directory) / 'benchmark.db'}", future=True)
            try:
                result = asyncio.run(
                    run_benchmark(
                        rows=max(args.rows, 1),
                        batch_size=max(args.batch_size, 1),
                        engine=sqlite_engine,
                    )
                )
            finally:
                sqlite_engine.dispose()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
