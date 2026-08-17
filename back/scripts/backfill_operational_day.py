from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, time
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from app.core.database import SessionLocal, init_db
from app.core.settings import get_settings
from app.models import AlarmHarvestRun, DeviceRecord, PublishedDashboardSnapshot
from app.schemas import BackfillRequest
from app.services.company_registry import CompanyRegistry
from app.services.dashboard import DashboardService
from app.services.ingestion import IngestionService
from app.services.realtime_hub import RealtimeHub


async def _run(
    company_slug: str,
    day_text: str,
    publish: bool,
    *,
    start_index: int,
    end_index: int | None,
    device_id: str | None,
    pause_between_devices: float | None,
    max_retries: int | None,
    base_cooldown: float | None,
    max_cooldown: float | None,
    maintenance: bool,
    maintenance_drain_timeout: float,
) -> None:
    settings = get_settings()
    if max_retries is not None:
        settings.backfill_rate_limit_max_retries = max(max_retries, 0)
    if base_cooldown is not None:
        settings.backfill_rate_limit_cooldown_seconds = max(base_cooldown, 1.0)
    if max_cooldown is not None:
        settings.backfill_rate_limit_max_cooldown_seconds = max(
            max_cooldown,
            settings.backfill_rate_limit_cooldown_seconds,
        )
    if pause_between_devices is not None:
        settings.howen_request_spacing_seconds = max(pause_between_devices, 0.0)
    init_db()
    registry = CompanyRegistry(
        settings.company_config_path,
        seed_path=settings.company_seed_config_path,
        session_factory=SessionLocal,
    )
    dashboard = DashboardService(session_factory=SessionLocal, registry=registry, settings=settings)
    ingestion = IngestionService(
        settings=settings,
        session_factory=SessionLocal,
        registry=registry,
        dashboard=dashboard,
        hub=RealtimeHub(),
    )
    maintenance_enabled = False
    maintenance_reason = f"manual_backfill:{company_slug}:{day_text}"

    company = registry.get(company_slug)
    timezone_name = company.timezone or settings.default_timezone
    local_tz = ZoneInfo(timezone_name)
    day_local = datetime.strptime(day_text, "%Y-%m-%d").date()
    today_local = datetime.now(local_tz).date()
    start_local = datetime.combine(day_local, time.min, local_tz)
    now_local = datetime.now(local_tz)
    if day_local >= now_local.date():
        end_local = now_local.replace(microsecond=0)
    else:
        end_local = datetime.combine(day_local, time.max.replace(microsecond=0), local_tz)

    try:
        if maintenance:
            await ingestion.set_maintenance_mode(enabled=True, reason=maintenance_reason)
            maintenance_enabled = True
            drain_deadline = asyncio.get_running_loop().time() + max(maintenance_drain_timeout, 0.0)
            while True:
                with SessionLocal() as session:
                    running_harvests = session.scalar(
                        select(func.count())
                        .select_from(AlarmHarvestRun)
                        .where(
                            AlarmHarvestRun.company_slug == company_slug,
                            AlarmHarvestRun.status == "running",
                        )
                    ) or 0
                if running_harvests <= 0 or asyncio.get_running_loop().time() >= drain_deadline:
                    print(
                        {
                            "maintenance": "enabled",
                            "reason": maintenance_reason,
                            "running_harvests": int(running_harvests),
                        },
                        flush=True,
                    )
                    break
                await asyncio.sleep(2.0)

        request = BackfillRequest(company_slug=company_slug, start_at=start_local, end_at=end_local)
        resolved_device_ids = ingestion._resolve_backfill_device_ids(request)

        if device_id:
            requested = device_id.strip()
            device_ids = [candidate for candidate in resolved_device_ids if candidate == requested]
        else:
            safe_start_index = max(start_index, 1)
            slice_start = safe_start_index - 1
            slice_end = end_index if end_index is not None and end_index >= safe_start_index else None
            device_ids = resolved_device_ids[slice_start:slice_end]

        total_inserted = 0
        total_anomalies = 0
        latest_observed_at: datetime | None = None
        failed_devices: list[dict[str, str | int | None]] = []
        effective_pause = max(float(settings.howen_request_spacing_seconds), 0.0)

        with SessionLocal() as session:
            device_lookup = {
                row.device_id: row.plate_no
                for row in session.scalars(select(DeviceRecord).where(DeviceRecord.device_id.in_(resolved_device_ids)))
            }

        print(
            {
                "company": company_slug,
                "date_local": day_text,
                "timezone": timezone_name,
                "devices_total": len(resolved_device_ids),
                "devices_selected": len(device_ids),
                "start_index": start_index,
                "end_index": end_index,
                "device_id": device_id,
                "pause_between_devices": effective_pause,
                "window_start_local": start_local.isoformat(),
                "window_end_local": end_local.isoformat(),
                "maintenance": maintenance,
            },
            flush=True,
        )

        if not device_ids:
            print({"summary": {"company": company_slug, "date_local": day_text, "devices_selected": 0}}, flush=True)
            return

        for index, device_id in enumerate(device_ids, start=1):
            plate_no = device_lookup.get(device_id)
            result = await ingestion._backfill_device_ids(
                device_ids=[device_id],
                start_at=start_local,
                end_at=end_local,
                source="harvest",
            )
            total_inserted += int(result.get("inserted", 0))
            total_anomalies += int(result.get("anomalies", 0))
            observed_text = result.get("latest_observed_at")
            if observed_text:
                observed_at = datetime.fromisoformat(observed_text.replace("Z", "+00:00"))
                if latest_observed_at is None or observed_at > latest_observed_at:
                    latest_observed_at = observed_at
            if result.get("failed_count", 0):
                failed_devices.append(
                    {
                        "device_id": device_id,
                        "plate_no": plate_no,
                        "failed_count": int(result.get("failed_count", 0)),
                    }
                )
            print(
                {
                    "progress": f"{index}/{len(device_ids)}",
                    "device_id": device_id,
                    "plate_no": plate_no,
                    "inserted": result.get("inserted", 0),
                    "anomalies": result.get("anomalies", 0),
                    "failed_count": result.get("failed_count", 0),
                    "latest_observed_at": result.get("latest_observed_at"),
                },
                flush=True,
            )
        summary = {
            "company": company_slug,
            "date_local": day_text,
            "devices_total": len(resolved_device_ids),
            "devices_selected": len(device_ids),
            "inserted": total_inserted,
            "anomalies": total_anomalies,
            "failed_devices": failed_devices,
            "latest_observed_at": latest_observed_at.isoformat() if latest_observed_at else None,
        }
        print({"summary": summary}, flush=True)

        if publish:
            with SessionLocal() as session:
                publication = session.get(PublishedDashboardSnapshot, company_slug)
                published_cut_at = publication.published_cut_at if publication else None
            if day_local < today_local and published_cut_at is not None:
                cut_at = published_cut_at
            else:
                cut_at = ingestion._latest_due_cut()
            payload = dashboard.materialize_snapshot(company_slug, cut_at=cut_at, cut_status="succeeded")
            print(
                {
                    "published_cut_at": payload.get("meta", {}).get("publishedCutAt"),
                    "recent_events": len(payload.get("recentEvents") or []),
                    "week_total": payload.get("dms", {}).get("semana", {}).get("total"),
                    "last_dms_event_at": payload.get("meta", {}).get("lastDmsEventAt"),
                },
                flush=True,
            )
    finally:
        if maintenance_enabled:
            await ingestion.set_maintenance_mode(enabled=False, reason=None)
            print({"maintenance": "disabled", "reason": maintenance_reason}, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill operativo de un dia local con progreso por dispositivo.")
    parser.add_argument("--company", required=True, help="Slug de la empresa")
    parser.add_argument("--date", required=True, help="Dia local YYYY-MM-DD")
    parser.add_argument("--publish", action="store_true", help="Publica snapshot al finalizar")
    parser.add_argument("--start-index", type=int, default=1, help="Indice 1-based del primer dispositivo a procesar")
    parser.add_argument("--end-index", type=int, default=None, help="Indice 1-based final exclusivo del tramo")
    parser.add_argument("--device-id", default=None, help="Procesa un solo device_id")
    parser.add_argument(
        "--pause-between-devices",
        type=float,
        default=None,
        help="Pausa entre dispositivos en segundos; por defecto usa howen_request_spacing_seconds",
    )
    parser.add_argument("--max-retries", type=int, default=None, help="Override de retries para rate limit")
    parser.add_argument("--base-cooldown", type=float, default=None, help="Override cooldown base en segundos")
    parser.add_argument("--max-cooldown", type=float, default=None, help="Override cooldown maximo en segundos")
    parser.add_argument(
        "--maintenance",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Activa maintenance mode mientras corre el backfill (default: true)",
    )
    parser.add_argument(
        "--maintenance-drain-timeout",
        type=float,
        default=60.0,
        help="Segundos maximos para esperar que terminen harvests corriendo antes de empezar",
    )
    args = parser.parse_args()
    asyncio.run(
        _run(
            args.company,
            args.date,
            args.publish,
            start_index=args.start_index,
            end_index=args.end_index,
            device_id=args.device_id,
            pause_between_devices=args.pause_between_devices,
            max_retries=args.max_retries,
            base_cooldown=args.base_cooldown,
            max_cooldown=args.max_cooldown,
            maintenance=args.maintenance,
            maintenance_drain_timeout=args.maintenance_drain_timeout,
        )
    )


if __name__ == "__main__":
    main()
