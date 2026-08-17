from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from app.core.database import SessionLocal, init_db
from app.core.settings import get_settings
from app.models import AlarmHarvestRun, PublishedDashboardSnapshot
from app.services.company_registry import CompanyRegistry
from app.services.dashboard import DashboardService
from app.services.ingestion import IngestionService
from app.services.realtime_hub import RealtimeHub


def _resolve_date_range(
    *,
    start_date_text: str | None,
    end_date_text: str | None,
    days: int,
    timezone_name: str,
) -> tuple[date, date]:
    local_tz = ZoneInfo(timezone_name)
    today_local = datetime.now(local_tz).date()
    if start_date_text:
        start_date = datetime.strptime(start_date_text, "%Y-%m-%d").date()
    else:
        start_date = today_local - timedelta(days=max(days, 1) - 1)
    if end_date_text:
        end_date = datetime.strptime(end_date_text, "%Y-%m-%d").date()
    else:
        end_date = today_local
    if end_date < start_date:
        raise ValueError("end_date no puede ser menor que start_date")
    return start_date, end_date


async def _run(
    company_slug: str,
    *,
    start_date_text: str | None,
    end_date_text: str | None,
    days: int,
    publish: bool,
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

    company = registry.get(company_slug)
    timezone_name = company.timezone or settings.default_timezone
    local_tz = ZoneInfo(timezone_name)
    today_local = datetime.now(local_tz).date()
    start_date, end_date = _resolve_date_range(
        start_date_text=start_date_text,
        end_date_text=end_date_text,
        days=days,
        timezone_name=timezone_name,
    )
    device_ids = ingestion._list_company_device_ids(company_slug)
    maintenance_enabled = False
    maintenance_reason = f"historical_rebuild:{company_slug}:{start_date.isoformat()}:{end_date.isoformat()}"

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

        print(
            {
                "company": company_slug,
                "timezone": timezone_name,
                "start_date_local": start_date.isoformat(),
                "end_date_local": end_date.isoformat(),
                "days_total": (end_date - start_date).days + 1,
                "devices_total": len(device_ids),
                "maintenance": maintenance,
            },
            flush=True,
        )

        current_date = start_date
        while current_date <= end_date:
            start_local = datetime.combine(current_date, time.min, local_tz)
            now_local = datetime.now(local_tz)
            if current_date >= now_local.date():
                end_local = now_local.replace(microsecond=0)
            else:
                end_local = datetime.combine(current_date, time.max.replace(microsecond=0), local_tz)
            result = await ingestion._backfill_device_ids(
                device_ids=device_ids,
                start_at=start_local,
                end_at=end_local,
                source="harvest",
            )
            print(
                {
                    "date_local": current_date.isoformat(),
                    "inserted": int(result.get("inserted", 0)),
                    "anomalies": int(result.get("anomalies", 0)),
                    "failed_count": int(result.get("failed_count", 0)),
                    "latest_observed_at": result.get("latest_observed_at"),
                },
                flush=True,
            )
            current_date += timedelta(days=1)

        if publish:
            with SessionLocal() as session:
                publication = session.get(PublishedDashboardSnapshot, company_slug)
                published_cut_at = publication.published_cut_at if publication else None
            if end_date < today_local and published_cut_at is not None:
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
    parser = argparse.ArgumentParser(description="Repuebla una empresa por rango de dias locales usando backfill historico seguro.")
    parser.add_argument("--company", required=True, help="Slug de la empresa")
    parser.add_argument("--start-date", default=None, help="Inicio local YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="Fin local YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=30, help="Dias hacia atras si no se envia rango explicito")
    parser.add_argument("--publish", action="store_true", help="Publica snapshot al finalizar")
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
        help="Activa maintenance mode mientras corre el rango (default: true)",
    )
    parser.add_argument(
        "--maintenance-drain-timeout",
        type=float,
        default=90.0,
        help="Segundos maximos para esperar que terminen harvests corriendo antes de empezar",
    )
    args = parser.parse_args()
    asyncio.run(
        _run(
            args.company,
            start_date_text=args.start_date,
            end_date_text=args.end_date,
            days=args.days,
            publish=args.publish,
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
