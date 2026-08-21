from __future__ import annotations

import argparse

from sqlalchemy import select

from app.core.database import SessionLocal
from app.core.settings import get_settings
from app.models import ReportAsset
from app.services.report_storage import ReportStorage


def main() -> None:
    parser = argparse.ArgumentParser(description="Move local report PDFs to configured object storage")
    parser.add_argument("--delete-local", action="store_true")
    args = parser.parse_args()
    settings = get_settings()
    storage = ReportStorage(settings)
    if storage.backend == "local":
        raise SystemExit("Object storage is not configured; set REPORT_STORAGE_BACKEND=supabase and service credentials")

    migrated = 0
    skipped = 0
    with SessionLocal() as session:
        reports = list(session.scalars(select(ReportAsset).order_by(ReportAsset.id)))
        for report in reports:
            if report.storage_backend == "supabase" and report.storage_key:
                skipped += 1
                continue
            payload = storage.read(backend="local", key=None, file_path=report.file_path)
            stored = storage.store(
                company_slug=report.company_slug,
                year=report.year,
                month=report.month,
                payload=payload,
                content_type="application/pdf",
            )
            old_path = report.file_path
            report.storage_backend = stored.backend
            report.storage_key = stored.key
            report.file_path = stored.file_path
            session.add(report)
            session.commit()
            if args.delete_local:
                storage.delete(backend="local", key=None, file_path=old_path)
            migrated += 1
    print({"migrated": migrated, "skipped": skipped, "backend": storage.backend})


if __name__ == "__main__":
    main()
