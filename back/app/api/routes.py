from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, File, Form, Header, HTTPException, Query, Request, Response, UploadFile, status
from fastapi.responses import FileResponse
from sqlalchemy import select

from app.api.deps import get_context, get_current_user, require_admin, resolve_company_slug
from app.models import ReportAsset
from app.schemas import (
    AuthMeResponse,
    BackfillRequest,
    CompanyAssignmentRequest,
    KmRepairRequest,
    LoginRequest,
    LoginResponse,
    ReconciliationRunRequest,
)

router = APIRouter()


@router.get("/health")
def healthcheck(request: Request) -> dict[str, str]:
    context = get_context(request)
    return {
        "status": "ok",
        "mode": context.settings.ingest_mode,
        "app": context.settings.app_name,
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
        secure=False,
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
    response.delete_cookie(context.settings.session_cookie_name, httponly=True, samesite="lax", secure=False)
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
def dashboard_snapshot(
    request: Request,
    company: str | None = Query(default=None),
) -> dict[str, object]:
    user = get_current_user(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    return context.dashboard.build_snapshot(company_slug)


@router.get("/dashboard/{company_slug}")
def dashboard_snapshot_by_slug(company_slug: str, request: Request) -> dict[str, object]:
    user = get_current_user(request)
    resolved_slug = resolve_company_slug(request=request, user=user, requested_slug=company_slug)
    context = get_context(request)
    return context.dashboard.build_snapshot(resolved_slug)


@router.get("/reports")
def list_reports(
    request: Request,
    company: str | None = Query(default=None),
) -> list[dict[str, object]]:
    payload = dashboard_snapshot(request, company)
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


@router.get("/admin/overview")
def admin_overview(
    request: Request,
    company: str | None = Query(default=None),
) -> dict[str, object]:
    user = require_admin(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    return context.dashboard.build_admin_overview(company_slug)


@router.get("/admin/live-setup")
def admin_live_setup(
    request: Request,
    company: str | None = Query(default=None),
) -> dict[str, object]:
    user = require_admin(request)
    company_slug = resolve_company_slug(request=request, user=user, requested_slug=company)
    context = get_context(request)
    return context.dashboard.build_admin_live_setup(company_slug)


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
    limit: int = Query(default=100, ge=1, le=500),
) -> list[dict[str, object]]:
    require_admin(request)
    context = get_context(request)
    return context.dashboard.list_anomalies(company_slug=company, limit=limit)


@router.post("/admin/backfill")
async def admin_backfill(request: Request, payload: BackfillRequest) -> dict[str, int]:
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
    return await context.ingestion.backfill_historical(payload)
