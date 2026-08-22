from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func, select, text

from app.bootstrap import build_context
from app.core.time import utc_now
from app.models import BackgroundJob, DeviceRecord, ManagedCompany
from app.schemas import HistoricalRebuildRequest


# These tables are derived from Howen and can be rebuilt. Identity, configuration,
# reports and lifecycle evidence are intentionally absent from this list.
OPERATIONAL_TABLES = (
    "reconciliation_job_devices",
    "reconciliation_jobs",
    "reconciliation_reviews",
    "alarm_harvest_devices",
    "alarm_harvest_runs",
    "company_historical_rebuild_jobs",
    "background_jobs",
    "catchup_cursor",
    "company_window_aggregates",
    "company_daily_aggregates",
    "published_dashboard_snapshots",
    "daily_mileage_snapshots",
    "mileage_observations",
    "mileage_readings",
    "alarm_events",
    "alarm_event_audit",
    "howen_alarm_raw",
    "ingestion_anomalies",
    "devices",
)

PRESERVED_TABLES = (
    "managed_companies",
    "user_accounts",
    "report_assets",
    "company_lifecycle_audit",
    "data_certification_runs",
    "system_settings",
    "alembic_version",
    "ingest_state",
)


def _table_counts(session: Any, table_names: tuple[str, ...]) -> dict[str, int]:
    return {
        table_name: int(session.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar_one())
        for table_name in table_names
    }


def _purge_operational_tables(session: Any) -> None:
    dialect = session.bind.dialect.name
    if dialect == "postgresql":
        table_sql = ", ".join(f'"{table_name}"' for table_name in OPERATIONAL_TABLES)
        session.execute(text(f"TRUNCATE TABLE {table_sql} RESTART IDENTITY"))
        return
    for table_name in OPERATIONAL_TABLES:
        session.execute(text(f'DELETE FROM "{table_name}"'))


def _reset_ingest_state(session: Any, *, maintenance_reason: str) -> None:
    session.execute(
        text(
            """
            UPDATE ingest_state
            SET mode = 'live',
                connection_state = 'maintenance',
                last_message_at = NULL,
                last_cycle_received_at = NULL,
                last_event_observed_at = NULL,
                last_status_at = NULL,
                last_alarm_at = NULL,
                last_live_alarm_message_at = NULL,
                last_live_dms_at = NULL,
                last_live_unmapped_at = NULL,
                last_device_sync_at = NULL,
                last_anomaly_at = NULL,
                maintenance_mode = TRUE,
                maintenance_reason = :maintenance_reason,
                maintenance_started_at = :started_at,
                last_error = NULL,
                updated_at = :started_at
            WHERE key = 'global'
            """
        ),
        {"maintenance_reason": maintenance_reason, "started_at": utc_now()},
    )


async def clean_rebuild(
    *,
    days: int,
    backup_path: str,
    manifest_path: Path,
) -> dict[str, Any]:
    if days < 1 or days > 40:
        raise ValueError("days debe estar entre 1 y 40")
    if not backup_path.strip():
        raise ValueError("backup_path es obligatorio")
    backup = Path(backup_path)
    if not backup.exists():
        raise ValueError(f"El respaldo no existe: {backup}")

    context = build_context(seed_users=False)
    context.registry.reload()
    maintenance_reason = f"clean_rebuild:{utc_now().strftime('%Y%m%dT%H%M%SZ')}"

    with context.session_factory() as session:
        active_companies = list(
            session.scalars(
                select(ManagedCompany)
                .where(ManagedCompany.is_active.is_(True))
                .order_by(ManagedCompany.slug.asc())
            )
        )
        company_slugs = [company.slug for company in active_companies]
        if not company_slugs:
            raise RuntimeError("No hay empresas activas para reconstruir")
        running_jobs = int(
            session.scalar(
                select(func.count())
                .select_from(BackgroundJob)
                .where(BackgroundJob.status == "running")
            )
            or 0
        )
        if running_jobs:
            raise RuntimeError(
                f"Hay {running_jobs} jobs ejecutandose. Deten el worker antes de continuar."
            )
        before = _table_counts(session, OPERATIONAL_TABLES)
        preserved_before = _table_counts(session, PRESERVED_TABLES)

    await context.ingestion.set_maintenance_mode(
        enabled=True,
        reason=maintenance_reason,
    )

    try:
        with context.session_factory() as session:
            _purge_operational_tables(session)
            _reset_ingest_state(session, maintenance_reason=maintenance_reason)
            session.commit()

        await context.ingestion.sync_devices(force=True)
        context.registry.reload()

        device_counts: dict[str, int] = {}
        rebuild_job_ids: dict[str, int] = {}
        for company_slug in company_slugs:
            with context.session_factory() as session:
                device_count = int(
                    session.scalar(
                        select(func.count())
                        .select_from(DeviceRecord)
                        .where(DeviceRecord.company_slug == company_slug)
                    )
                    or 0
                )
            if device_count <= 0:
                raise RuntimeError(
                    f"Howen no devolvio dispositivos para {company_slug}; se conserva mantenimiento para revision."
                )
            device_counts[company_slug] = device_count
            request = HistoricalRebuildRequest(
                company_slug=company_slug,
                days=days,
                publish_snapshot=True,
                maintenance=False,
            )
            rebuild_job_ids[company_slug] = context.ingestion.queue_historical_rebuild(
                request,
                spawn=False,
                purpose="activation_bootstrap",
            )

        with context.session_factory() as session:
            after = _table_counts(session, OPERATIONAL_TABLES)
            preserved_after = _table_counts(session, PRESERVED_TABLES)
        if preserved_after != preserved_before:
            raise RuntimeError(
                "Una tabla preservada cambio durante la limpieza; no se reactivara el worker."
            )

        await context.ingestion.set_maintenance_mode(enabled=False, reason=None)
        context.dashboard.clear_runtime_caches()
        context.ingestion.mark_dirty()

        result = {
            "status": "queued",
            "created_at": utc_now().isoformat(),
            "backup_path": str(backup),
            "days": days,
            "companies": company_slugs,
            "device_counts": device_counts,
            "rebuild_job_ids": rebuild_job_ids,
            "operational_counts_before": before,
            "operational_counts_after": after,
            "preserved_counts": preserved_after,
        }
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
        return result
    except Exception:
        # Maintenance remains enabled after any partial failure so the API cannot
        # present an incomplete clean rebuild as production data.
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Purga capas operativas reconstruibles y encola un rebuild limpio.",
    )
    parser.add_argument("--execute", action="store_true", help="Confirma la operacion destructiva")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--backup-path", required=True)
    parser.add_argument(
        "--manifest-path",
        default=(
            "storage/clean_rebuild_manifests/"
            f"clean-rebuild-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.execute:
        raise SystemExit("Operacion cancelada: falta --execute")
    result = asyncio.run(
        clean_rebuild(
            days=args.days,
            backup_path=args.backup_path,
            manifest_path=Path(args.manifest_path),
        )
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
