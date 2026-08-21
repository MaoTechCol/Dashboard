from __future__ import annotations

from dataclasses import dataclass

from app.core.database import SessionLocal, init_db
from app.core.settings import get_settings
from app.services.auth import AuthService
from app.services.company_registry import CompanyRegistry
from app.services.dashboard import DashboardService
from app.services.ingestion import IngestionService
from app.services.job_queue import JobQueue
from app.services.realtime_hub import RealtimeHub
from app.services.report_storage import ReportStorage


@dataclass
class AppContext:
    settings: object
    session_factory: object
    registry: CompanyRegistry
    dashboard: DashboardService
    hub: RealtimeHub
    ingestion: IngestionService
    jobs: JobQueue
    auth: AuthService
    report_storage: ReportStorage


def build_context(*, seed_users: bool) -> AppContext:
    settings = get_settings()
    settings.upload_dir.mkdir(parents=True, exist_ok=True)
    settings.session_cache_path.parent.mkdir(parents=True, exist_ok=True)
    init_db()

    registry = CompanyRegistry(
        settings.company_config_path,
        seed_path=settings.company_seed_config_path,
        session_factory=SessionLocal,
    )
    hub = RealtimeHub()
    auth = AuthService(session_factory=SessionLocal, settings=settings, registry=registry)
    report_storage = ReportStorage(settings)
    dashboard = DashboardService(session_factory=SessionLocal, registry=registry, settings=settings)
    ingestion = IngestionService(
        settings=settings,
        session_factory=SessionLocal,
        registry=registry,
        dashboard=dashboard,
        hub=hub,
    )
    jobs = JobQueue(session_factory=SessionLocal, settings=settings)
    if seed_users:
        auth.seed_users()
    return AppContext(
        settings=settings,
        session_factory=SessionLocal,
        registry=registry,
        dashboard=dashboard,
        hub=hub,
        ingestion=ingestion,
        jobs=jobs,
        auth=auth,
        report_storage=report_storage,
    )
