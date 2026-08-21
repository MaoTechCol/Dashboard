from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from app.api.routes import router as api_router
from app.api.ws import router as ws_router
from app.bootstrap import build_context
from app.core.database import is_database_timeout
from app.core.systemd import memory_monitor_loop, notify_systemd, watchdog_loop


logger = logging.getLogger("dashboard.api")


def create_app() -> FastAPI:
    context = build_context(seed_users=True)
    settings = context.settings

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
    app.state.context = context

    @app.middleware("http")
    async def _database_timeout_guard(request, call_next):
        try:
            return await call_next(request)
        except SQLAlchemyError as exc:
            if not is_database_timeout(exc):
                raise
            logger.warning(
                "api_database_query_cancelled method=%s path=%s timeout_ms=%s",
                request.method,
                request.url.path,
                settings.database_statement_timeout_ms,
            )
            return JSONResponse(
                status_code=504,
                headers={"Retry-After": "2"},
                content={
                    "detail": "La consulta excedio el tiempo seguro. Intenta nuevamente.",
                    "code": "database_query_timeout",
                },
            )

    @app.on_event("startup")
    async def _startup() -> None:
        app.state.systemd_stop = asyncio.Event()
        app.state.systemd_watchdog = asyncio.create_task(
            watchdog_loop(app.state.systemd_stop),
            name="api-systemd-watchdog",
        )
        app.state.memory_monitor = asyncio.create_task(
            memory_monitor_loop(
                app.state.systemd_stop,
                role=settings.process_role,
                warning_mb=settings.memory_warning_mb,
                critical_mb=settings.memory_critical_mb,
                interval_seconds=settings.memory_monitor_interval_seconds,
            ),
            name="api-memory-monitor",
        )
        if settings.process_role == "all":
            await context.ingestion.start()
        notify_systemd("READY=1\nSTATUS=API disponible")

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        notify_systemd("STOPPING=1")
        app.state.systemd_stop.set()
        app.state.systemd_watchdog.cancel()
        app.state.memory_monitor.cancel()
        with suppress(asyncio.CancelledError):
            await app.state.systemd_watchdog
        with suppress(asyncio.CancelledError):
            await app.state.memory_monitor
        if settings.process_role == "all":
            await context.ingestion.stop()

    return app


app = create_app()
