from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, File, Form, Header, HTTPException, Query, Request, Response, UploadFile, status
from fastapi.responses import FileResponse
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import get_context, get_current_user, require_admin, resolve_company_slug
from app.models import ReportAsset
from app.schemas import (
    AdminCompanyCatalogItemView,
    AdminCompanyCatalogView,
    AdminPasswordChangeRequest,
    AuthMeResponse,
    BackfillRequest,
    CompanyActivationRequest,
    CompanyAssignmentRequest,
    CompanyPasswordChangeRequest,
    HarvestRerunRequest,
    HistoricalRebuildRequest,
    KmRepairRequest,
    LoginRequest,
    LoginResponse,
    MaintenanceModeRequest,
    ReconciliationReviewBulkDecisionRequest,
    ReconciliationReviewDecisionRequest,
    ReconciliationRunRequest,
)
from app.services.howen import HowenRateLimitError

router = APIRouter()


@router.get("/health")
def healthcheck(request: Request) -> dict[str, str]:
    context = get_context(request)
    return {
        "status": "ok",
        "mode": context.settings.ingest_mode,
        "app": context.settings.app_name,
    }


@router.get("/healthz")
def healthz(request: Request) -> dict[str, str]:
    context = get_context(request)
    return {
        "status": "ok",
        "mode": context.settings.ingest_mode,
        "app": context.settings.app_name,
    }


@router.get("/readyz")
def readyz(request: Request, response: Response) -> dict[str, object]:
    context = get_context(request)
    checks: dict[str, bool] = {
        "database": False,
        "company_registry": False,
        "upload_dir": context.settings.upload_dir.exists(),
        "live_credentials": context.settings.ingest_mode != "live" or context.ingestion.howen.has_durable_credentials(),
    }
    diagnostics: dict[str, object] = {
        "connection_state": "unknown",
        "mode": context.settings.ingest_mode,
    }

    try:
        with context.session_factory() as session:
            if session.bind and session.bind.dialect.name.startswith("postgres"):
                session.execute(text("SET LOCAL statement_timeout = '1500ms'"))
            session.execute(select(1))
        checks["database"] = True
    except SQLAlchemyError as exc:
        diagnostics["database_error"] = str(exc)

    try:
        checks["company_registry"] = len(context.registry.all()) > 0
    except Exception as exc:  # pragma: no cover - defensive readiness guard
        diagnostics["registry_error"] = str(exc)

    ready = all(checks.values())
    response.status_code = status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE
    return {
        "status": "ready" if ready else "not_ready",
        "checks": checks,
        "diagnostics": diagnostics,
    }


@router.post("/auth/login")
def login(payload: LoginRequest, response: Response, request: Request) -> LoginResponse:
    context = get_context(request)
    user = context.auth.authenticate(payload.username.strip(), payload.password)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Credenciales invalidas")
    token = context.auth.create_session_token(user)
    response.set_cookie(
        context.settings.session_cookie_name,
        token,
        httponly=True,
        samesite="lax",
        secure=context.settings.session_cookie_secure,
        max_age=context.settings.session_ttl_minutes * 60,
    )
    companies = context.auth.serialize_companies(context.auth.visible_companies(user))
    return LoginResponse(
        user=context.auth.serialize_session(user),
        companies=companies,
        selected_company_slug=user.company_slug or (companies[0].slug if companies else None),
    )


@router.post("/auth/logout")
def logout(response: Response, request: Request) -> dict[str, bool]:
    context = get_context(request)
    response.delete_cookie(
        context.settings.session_cookie_name,
        httponly=True,
        samesite="lax",
        secure=context.settings.session_cookie_secure,
    )
    return {"ok": True}


@router.get("/auth/me")
def auth_me(request: Request) -> AuthMeResponse:
    context = get_context(request)
    user = get_current_user(request)
    companies = context.auth.serialize_companies(context.auth.visible_companies(user))
    return AuthMeResponse(
        user=context.auth.serialize_session(user),
        companies=companies,
        selected_company_slug=user.company_slug or (companies[0].slug if companies else None),
    )


@router.get("/companies")
def list_companies(request: Request) -> list[dict[str, object]]:
    context = get_context(request)
    user = get_current_user(request)
    return [company.model_dump(mode="json") for company in context.auth.visible_companies(user)]


@router.get("/feed")
def feed_status(
    request: Request,
    company: str | None = Query(default=None),
    known_cycle_at: datetime | None = Query(default=None),
) -> dict[str, object]:
    user = get_current_user(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    return context.dashboard.build_feed_poll(company_slug, known_cycle_at=known_cycle_at)


@router.get("/dashboard")
async def dashboard_snapshot(
    request: Request,
    company: str | None = Query(default=None),
    refresh: bool = Query(default=False),
) -> dict[str, object]:
    user = get_current_user(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    if refresh:
        await context.ingestion.refresh_snapshot(company_slug)
    return context.dashboard.build_snapshot(company_slug)


@router.get("/dashboard/{company_slug}")
async def dashboard_snapshot_by_slug(
    company_slug: str,
    request: Request,
    refresh: bool = Query(default=False),
) -> dict[str, object]:
    user = get_current_user(request)
    resolved_slug = resolve_company_slug(request=request, user=user, requested_slug=company_slug)
    context = get_context(request)
    if refresh:
        await context.ingestion.refresh_snapshot(resolved_slug)
    return context.dashboard.build_snapshot(resolved_slug)


@router.get("/reports")
def list_reports(
    request: Request,
    company: str | None = Query(default=None),
) -> list[dict[str, object]]:
    user = get_current_user(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    payload = context.dashboard.build_snapshot(company_slug)
    return payload["reports"]


@router.get("/reports/{year}/{month}")
def download_report(
    year: int,
    month: int,
    request: Request,
    company: str | None = Query(default=None),
):
    user = get_current_user(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    with context.session_factory() as session:
        report = session.scalar(
            select(ReportAsset).where(
                ReportAsset.company_slug == company_slug,
                ReportAsset.year == year,
                ReportAsset.month == month,
            )
        )
    if not report:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Reporte no encontrado")
    report_path = Path(report.file_path)
    if not report_path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="El archivo del reporte no existe")
    return FileResponse(report_path, media_type="application/pdf", filename=report.original_name)


@router.post("/admin/reports")
async def upload_report(
    request: Request,
    file: UploadFile = File(...),
    company_slug: str = Form(...),
    year: int = Form(...),
    month: int = Form(...),
    x_admin_token: str | None = Header(default=None),
) -> dict[str, object]:
    context = get_context(request)
    require_admin(request)
    company = context.registry.get(company_slug)
    if context.settings.admin_token and x_admin_token != context.settings.admin_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid admin token")
    if not context.registry.is_operational(company):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="La empresa no tiene flotas o vehiculos asignados")
    if file.content_type != "application/pdf" and not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Solo se aceptan reportes PDF")
    if month < 1 or month > 12:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="month must be between 1 and 12")
    today_local = context.dashboard.build_snapshot(company_slug)["meta"]["rangeEnd"]
    current_year, current_month, _ = [int(part) for part in today_local.split("-")]
    if (year, month) >= (current_year, current_month):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Solo se permiten meses cerrados")

    target_dir = context.settings.upload_dir / company_slug / str(year)
    target_dir.mkdir(parents=True, exist_ok=True)
    stored_name = f"{month:02d}.pdf"
    target_path = target_dir / stored_name
    payload = await file.read()
    target_path.write_bytes(payload)

    with context.session_factory() as session:
        report = session.scalar(
            select(ReportAsset).where(
                ReportAsset.company_slug == company_slug,
                ReportAsset.year == year,
                ReportAsset.month == month,
            )
        )
        if not report:
            report = ReportAsset(
                company_slug=company_slug,
                year=year,
                month=month,
                original_name=file.filename,
                stored_name=stored_name,
                file_path=str(target_path),
                size_bytes=len(payload),
            )
        else:
            report.original_name = file.filename
            report.stored_name = stored_name
            report.file_path = str(target_path)
            report.size_bytes = len(payload)
        session.add(report)
        session.commit()

    context.ingestion.mark_dirty()
    return {"ok": True, "path": str(target_path)}


@router.get("/admin/ingestion/status")
def admin_ingestion_status(
    request: Request,
    company: str | None = Query(default=None),
) -> dict[str, object]:
    user = require_admin(request)
    context = get_context(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company) if company else None
    return context.dashboard.build_admin_ingestion_status(company_slug=company_slug)


@router.post("/admin/ingestion/maintenance")
async def admin_ingestion_maintenance(
    request: Request,
    payload: MaintenanceModeRequest,
) -> dict[str, object]:
    require_admin(request)
    context = get_context(request)
    return await context.ingestion.set_maintenance_mode(
        enabled=payload.enabled,
        reason=payload.reason,
    )


@router.get("/admin/overview")
def admin_overview(
    request: Request,
    company: str | None = Query(default=None),
) -> dict[str, object]:
    context = get_context(request)
    require_admin(request)
    company_slug = company.strip() if company else None
    return context.dashboard.build_admin_overview(company_slug)


@router.get("/admin/companies")
def admin_companies(request: Request) -> dict[str, object]:
    require_admin(request)
    context = get_context(request)
    payload = AdminCompanyCatalogView.model_validate(context.dashboard.build_admin_company_catalog())
    return payload.model_dump(mode="json")


@router.post("/admin/companies")
async def admin_activate_company(request: Request, payload: CompanyActivationRequest) -> dict[str, object]:
    require_admin(request)
    context = get_context(request)
    try:
        company = context.registry.upsert_company(
            slug=payload.slug,
            name=payload.name,
            customer=payload.customer,
            timezone=payload.timezone,
            subdomain=payload.subdomain,
            fleet_ids=payload.fleet_ids,
            device_ids=payload.device_ids,
            notes=payload.notes,
        )
        context.auth.upsert_company_user(
            company_slug=company.slug,
            username=company.slug,
            password=payload.client_password,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    context.ingestion.queue_historical_rebuild(
        HistoricalRebuildRequest(
            company_slug=company.slug,
            days=30,
            publish_snapshot=True,
            maintenance=False,
        )
    )
    context.ingestion.mark_dirty()
    return admin_companies(request)


@router.post("/admin/companies/{company_slug}/deactivate")
async def admin_deactivate_company(company_slug: str, request: Request) -> dict[str, object]:
    require_admin(request)
    context = get_context(request)
    try:
        context.registry.get(company_slug)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    await context.ingestion.purge_company_operational_data(company_slug=company_slug)
    context.auth.delete_company_users(company_slug=company_slug)
    context.registry.delete_company(slug=company_slug)
    context.ingestion.mark_dirty()
    return admin_companies(request)


@router.post("/admin/users/admin/password")
def admin_change_own_password(
    request: Request,
    payload: AdminPasswordChangeRequest,
) -> dict[str, object]:
    user = require_admin(request)
    context = get_context(request)
    try:
        result = context.auth.change_password(
            username=user.username,
            new_password=payload.new_password,
            expected_role="admin",
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {
        "ok": True,
        "username": result.username,
        "role": result.role,
    }


@router.post("/admin/users/company/password")
def admin_change_company_password(
    request: Request,
    payload: CompanyPasswordChangeRequest,
) -> dict[str, object]:
    require_admin(request)
    context = get_context(request)
    try:
        company = context.registry.get(payload.company_slug)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    if not context.registry.is_operational(company):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="La empresa no esta activa")
    try:
        result = context.auth.upsert_company_user(
            company_slug=company.slug,
            username=company.slug,
            password=payload.new_password,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {
        "ok": True,
        "username": result.username,
        "role": result.role,
        "company_slug": result.company_slug,
    }


@router.get("/admin/live-setup")
def admin_live_setup(
    request: Request,
    company: str | None = Query(default=None),
    from_at: datetime | None = Query(default=None, alias="from"),
    to_at: datetime | None = Query(default=None, alias="to"),
) -> dict[str, object]:
    user = require_admin(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    return context.dashboard.build_admin_live_setup(company_slug, start_at=from_at, end_at=to_at)


@router.post("/admin/company-assignment")
def admin_company_assignment(request: Request, payload: CompanyAssignmentRequest) -> dict[str, object]:
    require_admin(request)
    context = get_context(request)
    if not payload.fleet_ids and not payload.device_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar fleet_ids o device_ids")
    context.registry.update_assignment(
        slug=payload.company_slug,
        fleet_ids=payload.fleet_ids,
        device_ids=payload.device_ids,
    )
    context.ingestion.mark_dirty()
    return context.dashboard.build_admin_live_setup(payload.company_slug)


@router.post("/admin/purge-mock")
def admin_purge_mock(request: Request) -> dict[str, object]:
    require_admin(request)
    context = get_context(request)
    result = context.dashboard.purge_mock_legacy()
    context.ingestion.mark_dirty()
    return result


@router.post("/admin/replay-status-anomalies")
async def admin_replay_status_anomalies(request: Request) -> dict[str, int]:
    require_admin(request)
    context = get_context(request)
    result = await context.ingestion.replay_status_anomalies()
    context.ingestion.mark_dirty()
    return result


@router.get("/admin/audit")
def admin_audit(
    request: Request,
    company: str | None = Query(default=None),
    from_at: datetime | None = Query(default=None, alias="from"),
    to_at: datetime | None = Query(default=None, alias="to"),
) -> dict[str, object]:
    user = require_admin(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    end_at = to_at or datetime.now().astimezone()
    start_at = from_at or (end_at - timedelta(days=7))
    if start_at >= end_at:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="from debe ser menor que to")
    return context.dashboard.build_admin_audit(company_slug, start_at=start_at, end_at=end_at)


@router.post("/admin/reconciliation/run")
async def admin_reconciliation_run(request: Request, payload: ReconciliationRunRequest) -> dict[str, object]:
    require_admin(request)
    context = get_context(request)
    return await context.dashboard.run_reconciliation(payload)


@router.get("/admin/reconciliation/jobs/{job_id}")
def admin_reconciliation_job(request: Request, job_id: str) -> dict[str, object]:
    require_admin(request)
    context = get_context(request)
    payload = context.dashboard.get_reconciliation_job(job_id)
    if not payload:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job de conciliacion no encontrado")
    return payload


@router.get("/admin/reconciliation/latest")
def admin_reconciliation_latest(
    request: Request,
    company: str | None = Query(default=None),
    from_at: datetime | None = Query(default=None, alias="from"),
    to_at: datetime | None = Query(default=None, alias="to"),
    window_type: str = Query(default="calendar_day_local"),
) -> dict[str, object]:
    user = require_admin(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    end_at = to_at or datetime.now().astimezone()
    start_at = from_at or (end_at - timedelta(days=1))
    if start_at >= end_at:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="from debe ser menor que to")
    payload = context.dashboard.get_latest_reconciliation(
        company_slug=company_slug,
        start_at=start_at,
        end_at=end_at,
        window_type=window_type,
    )
    if not payload:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No existe una conciliacion cacheada para ese rango")
    return payload


@router.get("/admin/reconciliation/summary")
async def admin_reconciliation_summary(
    request: Request,
    company: str | None = Query(default=None),
    from_at: datetime | None = Query(default=None, alias="from"),
    to_at: datetime | None = Query(default=None, alias="to"),
    window_type: str = Query(default="calendar_day_local"),
) -> dict[str, object]:
    user = require_admin(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    end_at = to_at or datetime.now().astimezone()
    start_at = from_at or (end_at - timedelta(days=1))
    if start_at >= end_at:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="from debe ser menor que to")
    return await context.dashboard.build_reconciliation_summary(
        company_slug=company_slug,
        start_at=start_at,
        end_at=end_at,
        window_type=window_type,
    )


@router.get("/admin/reconciliation/drilldown")
async def admin_reconciliation_drilldown(
    request: Request,
    company: str | None = Query(default=None),
    from_at: datetime | None = Query(default=None, alias="from"),
    to_at: datetime | None = Query(default=None, alias="to"),
    window_type: str = Query(default="calendar_day_local"),
) -> list[dict[str, object]]:
    user = require_admin(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    end_at = to_at or datetime.now().astimezone()
    start_at = from_at or (end_at - timedelta(days=1))
    if start_at >= end_at:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="from debe ser menor que to")
    return await context.dashboard.build_reconciliation_drilldown(
        company_slug=company_slug,
        start_at=start_at,
        end_at=end_at,
        window_type=window_type,
    )


@router.get("/admin/reconciliation/reviews")
def admin_reconciliation_reviews(
    request: Request,
    company: str | None = Query(default=None),
    from_at: datetime | None = Query(default=None, alias="from"),
    to_at: datetime | None = Query(default=None, alias="to"),
    status_filter: str = Query(default="pending", alias="status"),
    limit: int = Query(default=60, ge=1, le=200),
    sync_queue: bool = Query(default=False, alias="sync"),
    suggested_filter: str | None = Query(default=None, alias="suggested"),
) -> dict[str, object]:
    user = require_admin(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    end_at = to_at or datetime.now().astimezone()
    start_at = from_at or (end_at - timedelta(days=30))
    if start_at >= end_at:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="from debe ser menor que to")
    return context.dashboard.list_reconciliation_reviews(
        company_slug=company_slug,
        start_at=start_at,
        end_at=end_at,
        review_status=status_filter,
        limit=limit,
        sync_queue=sync_queue,
        suggested_actions=[
            item.strip()
            for item in (suggested_filter or "").split(",")
            if item.strip()
        ]
        or None,
    )


@router.post("/admin/reconciliation/reviews/{review_id}/approve")
def admin_reconciliation_review_approve(
    request: Request,
    review_id: int,
    payload: ReconciliationReviewDecisionRequest | None = None,
) -> dict[str, object]:
    user = require_admin(request)
    context = get_context(request)
    result = context.dashboard.decide_reconciliation_review(
        review_id=review_id,
        action="approve",
        decided_by=user.username,
        note=payload.note if payload else None,
    )
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Revision no encontrada")
    return result


@router.post("/admin/reconciliation/reviews/{review_id}/discard")
def admin_reconciliation_review_discard(
    request: Request,
    review_id: int,
    payload: ReconciliationReviewDecisionRequest | None = None,
) -> dict[str, object]:
    user = require_admin(request)
    context = get_context(request)
    result = context.dashboard.decide_reconciliation_review(
        review_id=review_id,
        action="discard",
        decided_by=user.username,
        note=payload.note if payload else None,
    )
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Revision no encontrada")
    return result


@router.post("/admin/reconciliation/reviews/bulk/approve")
def admin_reconciliation_review_bulk_approve(
    request: Request,
    payload: ReconciliationReviewBulkDecisionRequest,
) -> dict[str, object]:
    user = require_admin(request)
    context = get_context(request)
    return context.dashboard.decide_reconciliation_reviews_bulk(
        review_ids=payload.ids,
        action="approve",
        decided_by=user.username,
        note=payload.note,
    )


@router.post("/admin/reconciliation/reviews/bulk/discard")
def admin_reconciliation_review_bulk_discard(
    request: Request,
    payload: ReconciliationReviewBulkDecisionRequest,
) -> dict[str, object]:
    user = require_admin(request)
    context = get_context(request)
    return context.dashboard.decide_reconciliation_reviews_bulk(
        review_ids=payload.ids,
        action="discard",
        decided_by=user.username,
        note=payload.note,
    )


@router.get("/admin/km/quality")
def admin_km_quality(
    request: Request,
    company: str | None = Query(default=None),
) -> dict[str, object]:
    user = require_admin(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    return context.dashboard.build_km_quality(company_slug)


@router.post("/admin/km/repair")
def admin_km_repair(request: Request, payload: KmRepairRequest) -> dict[str, object]:
    require_admin(request)
    context = get_context(request)
    return context.dashboard.repair_km(payload)


@router.get("/admin/vehicles")
def admin_vehicles(
    request: Request,
    company: str | None = Query(default=None),
) -> list[dict[str, object]]:
    user = require_admin(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    return context.dashboard.list_vehicle_status(company_slug)


@router.get("/admin/anomalies")
def admin_anomalies(
    request: Request,
    company: str | None = Query(default=None),
    from_at: datetime | None = Query(default=None, alias="from"),
    to_at: datetime | None = Query(default=None, alias="to"),
    limit: int = Query(default=100, ge=1, le=500),
) -> list[dict[str, object]]:
    require_admin(request)
    context = get_context(request)
    return context.dashboard.list_anomalies(company_slug=company, start_at=from_at, end_at=to_at, limit=limit)


@router.get("/admin/raw-alarms")
def admin_raw_alarms(
    request: Request,
    company: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    source: str | None = Query(default=None),
    classification_status: str | None = Query(default=None),
    only_problematic: bool = Query(default=True),
) -> list[dict[str, object]]:
    user = require_admin(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    return context.dashboard.list_raw_alarm_diagnostics(
        company_slug=company_slug,
        limit=limit,
        source=source,
        classification_status=classification_status,
        only_problematic=only_problematic,
    )


@router.post("/admin/backfill")
async def admin_backfill(request: Request, payload: BackfillRequest) -> dict[str, object]:
    require_admin(request)
    context = get_context(request)
    if not payload.device_id and not payload.company_slug:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar company_slug o device_id")
    if payload.company_slug:
        company = context.registry.get(payload.company_slug)
        if not context.registry.is_operational(company):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="La empresa no esta operativa")
    if payload.start_at >= payload.end_at:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="start_at debe ser menor que end_at")
    try:
        return await context.ingestion.backfill_historical(payload)
    except HowenRateLimitError as exc:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=str(exc) or "Howen esta limitando solicitudes de backfill",
        ) from exc


@router.post("/admin/harvest/rerun-cut")
async def admin_rerun_harvest_cut(request: Request, payload: HarvestRerunRequest) -> dict[str, object]:
    user = require_admin(request)
    context = get_context(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=payload.company_slug)
    try:
        return await context.ingestion.rerun_harvest_cut(company_slug=company_slug, cut_at=payload.cut_at)
    except HowenRateLimitError as exc:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=str(exc) or "Howen esta limitando solicitudes del corte historico",
        ) from exc


@router.post("/admin/harvest/rebuild-history")
async def admin_rebuild_historical_window(request: Request, payload: HistoricalRebuildRequest) -> dict[str, object]:
    require_admin(request)
    context = get_context(request)
    company = context.registry.get(payload.company_slug)
    if not context.registry.is_operational(company):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="La empresa no esta operativa")
    try:
        return await context.ingestion.rebuild_historical_window(payload)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HowenRateLimitError as exc:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=str(exc) or "Howen esta limitando solicitudes de reconstruccion historica",
        ) from exc
