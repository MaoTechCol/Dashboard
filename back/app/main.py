from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router as api_router
from app.api.ws import router as ws_router
from app.bootstrap import build_context


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

    @app.on_event("startup")
    async def _startup() -> None:
        if settings.process_role == "all":
            await context.ingestion.start()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        if settings.process_role == "all":
            await context.ingestion.stop()

    return app


app = create_app()
