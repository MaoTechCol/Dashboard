from __future__ import annotations

from dataclasses import dataclass

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router as api_router
from app.api.ws import router as ws_router
from app.core.database import SessionLocal, init_db
from app.core.settings import get_settings
from app.services.auth import AuthService
from app.services.company_registry import CompanyRegistry
from app.services.dashboard import DashboardService
from app.services.ingestion import IngestionService
from app.services.realtime_hub import RealtimeHub


@dataclass
class AppContext:
    settings: object
    session_factory: object
    registry: CompanyRegistry
    dashboard: DashboardService
    hub: RealtimeHub
    ingestion: IngestionService
    auth: AuthService


def create_app() -> FastAPI:
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
    dashboard = DashboardService(session_factory=SessionLocal, registry=registry, settings=settings)
    ingestion = IngestionService(
        settings=settings,
        session_factory=SessionLocal,
        registry=registry,
        dashboard=dashboard,
        hub=hub,
    )
    auth.seed_users()

    app = FastAPI(title=settings.app_name)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.frontend_origins,
        allow_origin_regex=settings.frontend_origin_regex,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(api_router, prefix=settings.api_prefix)
    app.include_router(ws_router, prefix=settings.api_prefix)
    app.state.context = AppContext(
        settings=settings,
        session_factory=SessionLocal,
        registry=registry,
        dashboard=dashboard,
        hub=hub,
        ingestion=ingestion,
        auth=auth,
    )

    @app.on_event("startup")
    async def _startup() -> None:
        await ingestion.start()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await ingestion.stop()

    return app


app = create_app()
