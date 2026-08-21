from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
import os
import shutil
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from time import monotonic
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo

from sqlalchemy import and_, delete, func, insert, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.core.time import as_timezone, ensure_utc, parse_timestamp, to_local_date, utc_now
from app.models import AlarmEvent, AlarmEventAudit, AlarmHarvestDevice, AlarmHarvestRun, BackgroundJob, CatchupCursor, CompanyHistoricalRebuildJob, CompanyLifecycleAudit, DailyMileageSnapshot, DeviceRecord, HowenAlarmRaw, IngestState, IngestionAnomaly, ManagedCompany, MileageObservation, MileageReading, PublishedDashboardSnapshot, ReconciliationJob, ReconciliationJobDevice, ReconciliationReview, ReportAsset, UserAccount
from app.schemas import BackfillRequest, HistoricalRebuildRequest, NormalizedAlarm, NormalizedStatus
from app.services.company_registry import CompanyRegistry
from app.services.dashboard import DashboardService
from app.services.howen import HowenClient, HowenRateLimitError
from app.services.realtime_hub import RealtimeHub

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CatchupPlan:
    start_at: datetime
    end_at: datetime
    offset: int


@dataclass(frozen=True)
class PreparedAlarmRow:
    alarm: NormalizedAlarm
    provider_event_key: str | None
    company_slug: str | None
    fleet_id: str | None
    plate_no: str | None
    driver_name: str | None
    occurred_at: datetime | None
    received_at: datetime
    start_at: datetime | None
    end_at: datetime | None
    temporal_status: str
    temporal_resolution: str | None
    ingest_result: str
    payload_json: str

    @property
    def fuzzy_key(self) -> tuple[Any, ...]:
        return (
            self.alarm.device_id,
            self.alarm.category,
            self.alarm.raw_alarm_type,
            self.alarm.raw_tp,
            self.alarm.raw_event_code,
            ensure_utc(self.occurred_at),
        )

    @property
    def temporal_valid(self) -> bool:
        return self.temporal_status == "accepted"


@dataclass
class AlarmBatchResult:
    provider_rows: int = 0
    prepared_rows: int = 0
    raw_inserted: int = 0
    raw_updated: int = 0
    dms_inserted: int = 0
    dms_updated: int = 0
    duplicates: int = 0
    non_dms: int = 0
    unmapped: int = 0
    temporal_rejected: int = 0
    anomalies: int = 0
    errors: int = 0
    chunks_committed: int = 0
    latest_observed_at: datetime | None = None

    def merge(self, other: AlarmBatchResult) -> None:
        for field_name in (
            "provider_rows",
            "prepared_rows",
            "raw_inserted",
            "raw_updated",
            "dms_inserted",
            "dms_updated",
            "duplicates",
            "non_dms",
            "unmapped",
            "temporal_rejected",
            "anomalies",
            "errors",
            "chunks_committed",
        ):
            setattr(self, field_name, getattr(self, field_name) + getattr(other, field_name))
        self.latest_observed_at = _max_datetime(self.latest_observed_at, other.latest_observed_at)

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider_rows": self.provider_rows,
            "prepared_rows": self.prepared_rows,
            "raw_inserted": self.raw_inserted,
            "raw_updated": self.raw_updated,
            "dms_inserted": self.dms_inserted,
            "dms_updated": self.dms_updated,
            "duplicates": self.duplicates,
            "non_dms": self.non_dms,
            "unmapped": self.unmapped,
            "temporal_rejected": self.temporal_rejected,
            "anomalies": self.anomalies,
            "errors": self.errors,
            "chunks_committed": self.chunks_committed,
            "latest_observed_at": (
                ensure_utc(self.latest_observed_at).isoformat() if self.latest_observed_at else None
            ),
        }


@dataclass
class MileageRebuildResult:
    provider_rows: int = 0
    observations_written: int = 0
    snapshots_written: int = 0
    expected_device_days: int = 0
    valid_device_days: int = 0
    missing_device_days: int = 0
    review_cases: int = 0
    coverage_pct: float = 0.0
    publication_allowed: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider_rows": self.provider_rows,
            "observations_written": self.observations_written,
            "snapshots_written": self.snapshots_written,
            "expected_device_days": self.expected_device_days,
            "valid_device_days": self.valid_device_days,
            "missing_device_days": self.missing_device_days,
            "review_cases": self.review_cases,
            "coverage_pct": self.coverage_pct,
            "publication_allowed": self.publication_allowed,
        }


class HistoricalBackfillDeferred(RuntimeError):
    def __init__(self, *, next_retry_at: datetime, message: str) -> None:
        super().__init__(message)
        self.next_retry_at = next_retry_at


class IngestionService:
    def __init__(
        self,
        *,
        settings: Any,
        session_factory: Any,
        registry: CompanyRegistry,
        dashboard: DashboardService,
        hub: RealtimeHub,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.registry = registry
        self.dashboard = dashboard
        self.hub = hub
        self.howen = HowenClient(settings=settings, registry=registry)
        self._runner_task: asyncio.Task[None] | None = None
        self._publisher_task: asyncio.Task[None] | None = None
        self._harvest_task: asyncio.Task[None] | None = None
        self._harvest_scheduler_enabled = True
        self._dirty = asyncio.Event()
        self._last_purge_at = None
        self._last_device_sync_at = None
        self._catchup_locks: dict[str, asyncio.Lock] = {}
        self._harvest_locks: dict[str, asyncio.Lock] = {}
        self._cut_publish_locks: dict[datetime, asyncio.Lock] = {}
        self._historical_rebuild_tasks: dict[int, asyncio.Task[Any]] = {}
        self._evidence_fetch_tasks: dict[tuple[str, str], asyncio.Task[list[dict[str, Any]]]] = {}

    def _historical_rebuild_max_concurrency(self) -> int:
        return max(int(getattr(self.settings, "historical_rebuild_max_concurrency", 1) or 1), 1)

    def _historical_rebuild_running_count(self) -> int:
        return sum(1 for task in self._historical_rebuild_tasks.values() if not task.done())

    def _can_start_historical_rebuild(self) -> bool:
        return self._historical_rebuild_running_count() < self._historical_rebuild_max_concurrency()

    async def start(
        self,
        *,
        include_harvest_scheduler: bool = True,
        include_realtime_publisher: bool = True,
        resume_historical_rebuilds: bool = True,
    ) -> None:
        self._harvest_scheduler_enabled = include_harvest_scheduler
        self._ensure_state_row()
        self._recover_stale_harvest_runs()
        self._runner_task = asyncio.create_task(self._run_live_forever(), name="ingestion-runner")
        if include_realtime_publisher:
            self._publisher_task = asyncio.create_task(self._publisher_loop(), name="dashboard-publisher")
        if include_harvest_scheduler:
            self._harvest_task = asyncio.create_task(self._harvest_loop(), name="dashboard-harvest")
            asyncio.create_task(self._run_due_harvests(), name="dashboard-harvest-startup")
        if resume_historical_rebuilds:
            await self._resume_due_historical_rebuilds()
        if include_realtime_publisher:
            self.mark_dirty()
        else:
            self.dashboard.clear_runtime_caches()

    def latest_due_cut(self) -> datetime:
        return self._latest_due_cut()

    def due_harvest_cuts(self) -> list[tuple[str, datetime]]:
        self.registry.reload()
        latest_due_cut = self._latest_due_cut()
        pending: list[tuple[str, datetime]] = []
        for company in self.registry.all():
            if not self.registry.is_operational(company) or self._activation_bootstrap_running(company.slug):
                continue
            with self.session_factory() as session:
                publication = session.get(PublishedDashboardSnapshot, company.slug)
                last_cut = ensure_utc(publication.published_cut_at) if publication and publication.published_cut_at else None
            if last_cut is None:
                pending.append((company.slug, latest_due_cut))
                continue
            if last_cut < latest_due_cut:
                # The newest cut absorbs any gap. Queueing every missed quarter only
                # multiplies provider calls and delays recovery after an outage.
                pending.append((company.slug, latest_due_cut))
        return pending

    async def run_harvest_cut(self, *, company_slug: str, cut_at: datetime, force: bool = False) -> dict[str, Any]:
        self.registry.reload()
        return await self._run_harvest_for_cut(company_slug=company_slug, cut_at=cut_at, force=force)

    def is_cut_superseded(self, *, company_slug: str, cut_at: datetime) -> bool:
        requested_cut = ensure_utc(cut_at)
        if requested_cut is None:
            return False
        with self.session_factory() as session:
            publication = session.get(PublishedDashboardSnapshot, company_slug)
            published_cut = (
                ensure_utc(publication.published_cut_at)
                if publication and publication.published_cut_at
                else None
            )
        return published_cut is not None and published_cut > requested_cut

    async def stop(self) -> None:
        tasks = [
            task
            for task in (
                self._runner_task,
                self._publisher_task,
                self._harvest_task,
                *self._historical_rebuild_tasks.values(),
                *self._evidence_fetch_tasks.values(),
            )
            if task
        ]
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        try:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=2.0)
        except asyncio.TimeoutError:
            pass
        finally:
            self._runner_task = None
            self._publisher_task = None
            self._harvest_task = None
            self._historical_rebuild_tasks.clear()
            self._evidence_fetch_tasks.clear()

    def critical_runtime_tasks(self) -> tuple[asyncio.Task[None], ...]:
        return tuple(
            task
            for task in (self._runner_task, self._publisher_task)
            if task is not None
        )

    def mark_dirty(self) -> None:
        self.dashboard.clear_runtime_caches()
        self._dirty.set()

    def _spawn_historical_rebuild_task(
        self,
        *,
        request: HistoricalRebuildRequest,
        rebuild_job_id: int,
    ) -> None:
        existing_task = self._historical_rebuild_tasks.get(rebuild_job_id)
        if existing_task and not existing_task.done():
            return
        task = asyncio.create_task(
            self.rebuild_historical_window(request, rebuild_job_id=rebuild_job_id),
            name=f"historical-rebuild-{request.company_slug}-{rebuild_job_id}",
        )
        self._historical_rebuild_tasks[rebuild_job_id] = task

        def _cleanup(completed: asyncio.Task[Any]) -> None:
            current = self._historical_rebuild_tasks.get(rebuild_job_id)
            if current is completed:
                self._historical_rebuild_tasks.pop(rebuild_job_id, None)
            with suppress(asyncio.CancelledError):
                exc = completed.exception()
                if exc is not None:
                    logger.error(
                        "Historical rebuild task failed for %s (job %s): %s",
                        request.company_slug,
                        rebuild_job_id,
                        exc,
                        exc_info=(type(exc), exc, exc.__traceback__),
                    )
            with suppress(RuntimeError):
                asyncio.create_task(
                    self._resume_due_historical_rebuilds(),
                    name="historical-rebuild-resume",
                )

        task.add_done_callback(_cleanup)

    async def _resume_due_historical_rebuilds(self) -> None:
        available_slots = self._historical_rebuild_max_concurrency() - self._historical_rebuild_running_count()
        if available_slots <= 0:
            return
        now_utc = utc_now()
        with self.session_factory() as session:
            pending_jobs = list(
                session.scalars(
                    select(CompanyHistoricalRebuildJob)
                    .where(
                        CompanyHistoricalRebuildJob.purpose == "activation_bootstrap",
                        CompanyHistoricalRebuildJob.status.in_(("queued", "running")),
                    )
                    .order_by(CompanyHistoricalRebuildJob.created_at.asc(), CompanyHistoricalRebuildJob.id.asc())
                )
            )
        for job in pending_jobs:
            if available_slots <= 0:
                break
            if job.next_retry_at and (ensure_utc(job.next_retry_at) or now_utc) > now_utc:
                continue
            request = HistoricalRebuildRequest(
                company_slug=job.company_slug,
                start_date=job.start_date,
                end_date=job.end_date,
                publish_snapshot=True,
                maintenance=False,
            )
            self._spawn_historical_rebuild_task(request=request, rebuild_job_id=job.id)
            available_slots -= 1

    def _recover_stale_harvest_runs(self) -> None:
        self._cleanup_orphan_harvest_runs(include_current_cut=True)

    def _cleanup_orphan_harvest_runs(self, *, include_current_cut: bool) -> None:
        recovered_at = utc_now()
        current_cut_at = self._latest_due_cut(recovered_at)
        with self.session_factory() as session:
            bootstrap_companies = set(
                session.scalars(
                    select(CompanyHistoricalRebuildJob.company_slug).where(
                        CompanyHistoricalRebuildJob.purpose == "activation_bootstrap",
                        CompanyHistoricalRebuildJob.status.in_(("queued", "running")),
                    )
                )
            )
            conditions: list[Any] = []
            if include_current_cut:
                conditions.append(True)
            else:
                conditions.append(AlarmHarvestRun.cut_at < current_cut_at)
            if bootstrap_companies:
                conditions.append(AlarmHarvestRun.company_slug.in_(bootstrap_companies))
            stale_runs = list(
                session.scalars(
                    select(AlarmHarvestRun).where(AlarmHarvestRun.status.in_(("queued", "running")), or_(*conditions))
                )
            )
            if not stale_runs:
                return
            affected_companies = {row.company_slug for row in stale_runs}
            for run in stale_runs:
                run.status = "failed"
                run.finished_at = recovered_at
                run.error_message = run.error_message or "Recovered after service restart"
                session.add(run)
            for company_slug in affected_companies:
                publication = session.get(PublishedDashboardSnapshot, company_slug)
                if publication and publication.cut_status in {"queued", "running"}:
                    publication.cut_status = "failed"
                    publication.last_error = publication.last_error or "Recovered after service restart"
                    session.add(publication)
            session.commit()
        self.dashboard.clear_runtime_caches()

    def get_maintenance_state(self) -> dict[str, Any]:
        with self.session_factory() as session:
            state = session.get(IngestState, "global")
            started_at = ensure_utc(state.maintenance_started_at) if state else None
            return {
                "enabled": bool(state.maintenance_mode) if state else False,
                "reason": state.maintenance_reason if state else None,
                "started_at": started_at.isoformat() if started_at else None,
            }

    async def set_maintenance_mode(
        self,
        *,
        enabled: bool,
        reason: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        with self.session_factory() as session:
            state = session.get(IngestState, "global")
            if not state:
                state = IngestState(key="global", mode="live", connection_state="idle")
                session.add(state)
            if enabled:
                state.maintenance_mode = True
                state.maintenance_reason = (reason or "").strip() or "manual_maintenance"
                state.maintenance_started_at = ensure_utc(state.maintenance_started_at) or now
            else:
                state.maintenance_mode = False
                state.maintenance_reason = None
                state.maintenance_started_at = None
            session.commit()
        self.dashboard.clear_runtime_caches()
        self.mark_dirty()
        return self.get_maintenance_state()

    def _maintenance_active(self) -> bool:
        with self.session_factory() as session:
            state = session.get(IngestState, "global")
            return bool(state.maintenance_mode) if state else False

    async def _publisher_loop(self) -> None:
        while True:
            await self._dirty.wait()
            self._dirty.clear()
            await asyncio.sleep(0.4)
            await self._purge_if_needed()
            with self.session_factory() as session:
                published_company_slugs = set(
                    session.scalars(
                        select(PublishedDashboardSnapshot.company_slug).where(PublishedDashboardSnapshot.snapshot_json.is_not(None))
                    )
                )
            for company in self.registry.all():
                if not self.registry.is_operational(company):
                    continue
                if company.slug not in published_company_slugs:
                    continue
                payload = self.dashboard.build_snapshot(company.slug)
                await self.hub.publish(company.slug, payload)

    async def _harvest_loop(self) -> None:
        interval_seconds = max(int(self.settings.harvest_check_interval_seconds), 5)
        while True:
            await asyncio.sleep(interval_seconds)
            if self._maintenance_active():
                continue
            try:
                await self._resume_due_historical_rebuilds()
                await self._run_due_harvests()
            except asyncio.CancelledError:
                raise
            except Exception:
                continue

    async def refresh_snapshot(self, company_slug: str) -> dict[str, Any]:
        latest_due_cut = self._latest_due_cut()
        with self.session_factory() as session:
            run = session.scalar(
                select(AlarmHarvestRun).where(
                    AlarmHarvestRun.company_slug == company_slug,
                    AlarmHarvestRun.cut_at == latest_due_cut,
                )
            )
            publication = session.get(PublishedDashboardSnapshot, company_slug)
            published_cut_at = ensure_utc(publication.published_cut_at) if publication and publication.published_cut_at else None
        if self._maintenance_active():
            return self.dashboard.build_snapshot(company_slug)
        if run and run.status == "succeeded":
            if published_cut_at is None or published_cut_at < latest_due_cut:
                self.dashboard.materialize_snapshot(company_slug, cut_at=latest_due_cut, cut_status="succeeded")
        elif not run or run.status not in {"running", "queued"}:
            asyncio.create_task(
                self._run_harvest_for_cut(company_slug=company_slug, cut_at=latest_due_cut, force=False),
                name=f"dashboard-refresh-{company_slug}-{latest_due_cut.isoformat()}",
            )
        return self.dashboard.build_snapshot(company_slug)

    async def rerun_harvest_cut(self, *, company_slug: str, cut_at: datetime) -> dict[str, Any]:
        return await self._run_harvest_for_cut(
            company_slug=company_slug,
            cut_at=ensure_utc(cut_at) or self._latest_due_cut(),
            force=True,
        )

    def _resolve_historical_rebuild_range(
        self,
        *,
        company_slug: str,
        start_date_value: date | None,
        end_date_value: date | None,
        days: int,
    ) -> tuple[date, date, ZoneInfo]:
        company = self.registry.get(company_slug)
        company_tz = ZoneInfo(company.timezone or self.settings.default_timezone)
        today_local = utc_now().astimezone(company_tz).date()
        start_date_local = start_date_value or (today_local - timedelta(days=max(days, 1) - 1))
        end_date_local = end_date_value or today_local
        if end_date_local < start_date_local:
            raise ValueError("end_date no puede ser menor que start_date")
        return start_date_local, end_date_local, company_tz

    def _activation_bootstrap_running(self, company_slug: str) -> bool:
        with self.session_factory() as session:
            job = session.scalars(
                select(CompanyHistoricalRebuildJob)
                .where(
                    CompanyHistoricalRebuildJob.company_slug == company_slug,
                    CompanyHistoricalRebuildJob.purpose == "activation_bootstrap",
                    CompanyHistoricalRebuildJob.status.in_(("queued", "running", "awaiting_approval")),
                )
                .order_by(CompanyHistoricalRebuildJob.created_at.desc(), CompanyHistoricalRebuildJob.id.desc())
            ).first()
        return job is not None

    def _resolve_safe_publish_cut_for_range(
        self,
        *,
        company_slug: str,
        range_end_at: datetime,
    ) -> datetime:
        company = self.registry.get(company_slug)
        company_tz = ZoneInfo(company.timezone or self.settings.default_timezone)
        latest_due_cut = self._latest_due_cut()
        range_end_utc = ensure_utc(range_end_at) or latest_due_cut
        today_local = utc_now().astimezone(company_tz).date()
        range_end_local = range_end_utc.astimezone(company_tz).date()
        with self.session_factory() as session:
            publication = session.get(PublishedDashboardSnapshot, company_slug)
            published_cut_at = ensure_utc(publication.published_cut_at) if publication and publication.published_cut_at else None
        if range_end_local < today_local and published_cut_at is not None:
            return published_cut_at
        if published_cut_at is not None and published_cut_at > latest_due_cut:
            return published_cut_at
        return latest_due_cut

    def _resolve_safe_publish_cut_for_harvest(
        self,
        *,
        company_slug: str,
        harvested_cut_at: datetime,
    ) -> datetime:
        harvested_cut_at = ensure_utc(harvested_cut_at) or self._latest_due_cut()
        with self.session_factory() as session:
            publication = session.get(PublishedDashboardSnapshot, company_slug)
            published_cut_at = ensure_utc(publication.published_cut_at) if publication and publication.published_cut_at else None
        if published_cut_at is not None and published_cut_at > harvested_cut_at:
            return published_cut_at
        return harvested_cut_at

    def _harvest_interval(self) -> timedelta:
        return timedelta(minutes=max(int(self.settings.harvest_cut_interval_minutes), 1))

    def _latest_due_cut(self, now_at: datetime | None = None) -> datetime:
        now_utc = ensure_utc(now_at) or utc_now()
        lagged = now_utc - timedelta(seconds=max(int(self.settings.harvest_window_lag_seconds), 0))
        interval_seconds = int(self._harvest_interval().total_seconds())
        aligned_seconds = int(lagged.timestamp()) // interval_seconds * interval_seconds
        return datetime.fromtimestamp(aligned_seconds, tz=ZoneInfo("UTC"))

    def _harvest_window_for_cut(
        self,
        cut_at: datetime,
        *,
        company_slug: str | None = None,
    ) -> tuple[datetime, datetime]:
        cut_at = ensure_utc(cut_at) or utc_now()
        previous_cut = cut_at - self._harvest_interval()
        overlap = timedelta(minutes=max(int(self.settings.harvest_overlap_minutes), 0))
        window_start = previous_cut - overlap
        if company_slug:
            with self.session_factory() as session:
                publication = session.get(PublishedDashboardSnapshot, company_slug)
                published_cut = (
                    ensure_utc(publication.published_cut_at)
                    if publication and publication.published_cut_at
                    else None
                )
            if published_cut is not None and published_cut < previous_cut:
                window_start = min(window_start, published_cut - overlap)
        return window_start, cut_at

    async def _yield_to_ready_harvests(self) -> None:
        if self._maintenance_active():
            return
        await asyncio.sleep(0)
        while True:
            with self.session_factory() as session:
                active_harvest = session.scalar(
                    select(BackgroundJob.id)
                    .where(
                        BackgroundJob.job_type.in_(("harvest_cut", "refresh_snapshot")),
                        BackgroundJob.status.in_(("queued", "running")),
                    )
                    .limit(1)
                )
            if active_harvest is None:
                return
            await asyncio.sleep(0.5)

    def _official_harvest_cohort(self) -> list[str]:
        self.registry.reload()
        return sorted(
            company.slug
            for company in self.registry.all()
            if self.registry.is_operational(company)
            and not self._activation_bootstrap_running(company.slug)
        )

    async def _publish_harvest_cohort_if_ready(self, *, cut_at: datetime) -> dict[str, Any]:
        cut_at = ensure_utc(cut_at) or self._latest_due_cut()
        lock = self._cut_publish_locks.setdefault(cut_at, asyncio.Lock())
        async with lock:
            cohort = self._official_harvest_cohort()
            ready: list[str] = []
            missing: list[str] = []
            with self.session_factory() as session:
                for company_slug in cohort:
                    publication = session.get(PublishedDashboardSnapshot, company_slug)
                    published_cut = (
                        ensure_utc(publication.published_cut_at)
                        if publication and publication.published_cut_at
                        else None
                    )
                    if published_cut is not None and published_cut >= cut_at:
                        ready.append(company_slug)
                        continue
                    succeeded_run = session.scalar(
                        select(AlarmHarvestRun.id)
                        .where(
                            AlarmHarvestRun.company_slug == company_slug,
                            AlarmHarvestRun.cut_at == cut_at,
                            AlarmHarvestRun.status == "succeeded",
                        )
                        .limit(1)
                    )
                    if succeeded_run is None:
                        missing.append(company_slug)
                    else:
                        ready.append(company_slug)

            if missing:
                return {
                    "cohort_ready": False,
                    "cohort_published": False,
                    "cohort_companies": cohort,
                    "cohort_waiting_for": missing,
                }

            published: list[str] = []
            for company_slug in cohort:
                with self.session_factory() as session:
                    publication = session.get(PublishedDashboardSnapshot, company_slug)
                    published_cut = (
                        ensure_utc(publication.published_cut_at)
                        if publication and publication.published_cut_at
                        else None
                    )
                if published_cut is not None and published_cut >= cut_at:
                    continue
                payload = await asyncio.to_thread(
                    self.dashboard.materialize_snapshot,
                    company_slug,
                    cut_at=cut_at,
                    cut_status="succeeded",
                )
                await self.hub.publish(company_slug, payload)
                published.append(company_slug)
            if published:
                self.mark_dirty()
            return {
                "cohort_ready": True,
                "cohort_published": bool(published),
                "cohort_companies": cohort,
                "cohort_published_companies": published,
                "cohort_waiting_for": [],
            }

    async def _update_rebuild_progress(
        self,
        *,
        rebuild_job_id: int,
        days_total: int,
        completed_days_offset: int,
        chunk_start_date: date,
        chunk_end_date: date,
        processed_devices: int,
        devices_total: int,
    ) -> None:
        chunk_days_total = max((chunk_end_date - chunk_start_date).days + 1, 1)
        if devices_total <= 0:
            approx_chunk_days = chunk_days_total
        else:
            approx_chunk_days = min(
                chunk_days_total,
                max(1, (processed_devices * chunk_days_total + devices_total - 1) // devices_total),
            )
        approx_days_done = min(days_total, completed_days_offset + approx_chunk_days)
        with self.session_factory() as session:
            rebuild_job = session.get(CompanyHistoricalRebuildJob, rebuild_job_id)
            if not rebuild_job:
                return
            current_days_done = rebuild_job.days_done or 0
            if approx_days_done <= current_days_done:
                return
            rebuild_job.days_done = approx_days_done
            session.add(rebuild_job)
            session.commit()

    async def rebuild_authoritative_mileage(
        self,
        *,
        company_slug: str,
        start_date_local: date,
        end_date_local: date,
        company_tz: ZoneInfo,
        device_ids: list[str],
        rebuild_job_id: int | None,
    ) -> MileageRebuildResult:
        """Rebuild daily mileage from the same aggregate report used by VSS."""
        result = MileageRebuildResult()
        now_utc = utc_now()
        today_local = now_utc.astimezone(company_tz).date()
        effective_end_date = min(end_date_local, today_local)
        if effective_end_date < start_date_local or not device_ids:
            result.publication_allowed = True
            return result

        report_start = datetime.combine(start_date_local, time.min, company_tz)
        report_end = (
            now_utc.astimezone(company_tz).replace(microsecond=0)
            if effective_end_date == today_local
            else datetime.combine(effective_end_date, time.max.replace(microsecond=0), company_tz)
        )
        if rebuild_job_id is not None:
            self._set_rebuild_phase(
                rebuild_job_id,
                phase="km_provider",
                mileage_days_total=(effective_end_date - start_date_local).days + 1,
                mileage_devices_total=len(device_ids),
            )

        try:
            rows = await self.howen.fetch_daily_mileage_report_authorized(
                device_ids=device_ids,
                start_at=report_start,
                end_at=report_end,
            )
        except HowenRateLimitError as exc:
            cooldown = max(float(self.settings.backfill_rate_limit_cooldown_seconds), 5.0)
            raise HistoricalBackfillDeferred(
                next_retry_at=now_utc + timedelta(seconds=cooldown),
                message=f"Proveedor limitando la reconstruccion de kilometraje: {exc}",
            ) from exc

        result.provider_rows = len(rows)
        all_dates = list(_date_range(start_date_local, effective_end_date))
        result.expected_device_days = len(device_ids) * len(all_dates)

        if rebuild_job_id is not None:
            self._set_rebuild_phase(
                rebuild_job_id,
                phase="km_validation",
                mileage_rows_total=result.expected_device_days,
            )

        with self.session_factory() as session:
            devices = list(
                session.scalars(select(DeviceRecord).where(DeviceRecord.device_id.in_(device_ids)))
            )
            devices_by_id = {device.device_id: device for device in devices}
            device_by_plate = {
                str(device.plate_no or "").strip().upper(): device
                for device in devices
                if str(device.plate_no or "").strip()
            }
            report_by_device: dict[str, dict[date, tuple[float, Any, dict[str, Any]]]] = {}
            for row in rows:
                device_id = _mileage_report_device_id(row)
                if not device_id:
                    plate_candidate = _mileage_report_plate(row)
                    matched = device_by_plate.get(plate_candidate.upper()) if plate_candidate else None
                    device_id = matched.device_id if matched else None
                if not device_id or device_id not in devices_by_id:
                    continue
                values = _mileage_report_daily_values(
                    row,
                    start_date=start_date_local,
                    end_date=effective_end_date,
                )
                if values:
                    report_by_device.setdefault(device_id, {}).update(
                        {day: (value, raw_value, row) for day, (value, raw_value) in values.items()}
                    )

            existing_snapshots = {
                (snapshot.device_id, snapshot.snapshot_date): snapshot
                for snapshot in session.scalars(
                    select(DailyMileageSnapshot).where(
                        DailyMileageSnapshot.device_id.in_(device_ids),
                        DailyMileageSnapshot.snapshot_date >= start_date_local,
                        DailyMileageSnapshot.snapshot_date <= effective_end_date,
                    )
                )
            }
            existing_observations = {
                observation.observation_key: observation
                for observation in session.scalars(
                    select(MileageObservation).where(
                        MileageObservation.company_slug == company_slug,
                        MileageObservation.source == "howen_daily_report",
                        MileageObservation.observed_at >= report_start.astimezone(ZoneInfo("UTC")),
                        MileageObservation.observed_at <= report_end.astimezone(ZoneInfo("UTC")),
                    )
                )
            }
            tolerance = max(float(self.settings.mileage_source_disagreement_tolerance_km), 0.0)
            max_daily_km = max(float(self.settings.mileage_max_daily_km), 1.0)

            for device_id in device_ids:
                device = devices_by_id.get(device_id)
                plate_no = device.plate_no if device else None
                fleet_id = device.fleet_id if device else None
                values = report_by_device.get(device_id, {})
                for snapshot_date in all_dates:
                    report_value = values.get(snapshot_date)
                    if report_value is None:
                        result.missing_device_days += 1
                        if self._ensure_km_review(
                            session,
                            company_slug=company_slug,
                            device_id=device_id,
                            plate_no=plate_no,
                            snapshot_date=snapshot_date,
                            reason="missing_day_km",
                            diagnostic_note=(
                                "Howen no devolvio evidencia de kilometraje diario para este vehiculo y fecha. "
                                "No se publica como 0 km."
                            ),
                            payload={"source": "howen_daily_report", "status": "missing"},
                        ):
                            result.review_cases += 1
                        continue

                    day_km, raw_value, source_row = report_value
                    observed_local = (
                        now_utc.astimezone(company_tz)
                        if snapshot_date == today_local
                        else datetime.combine(snapshot_date, time.max.replace(microsecond=0), company_tz)
                    )
                    observed_at = observed_local.astimezone(ZoneInfo("UTC"))
                    validation_status = "valid"
                    validation_reason: str | None = None
                    if day_km < 0:
                        validation_status = "invalid"
                        validation_reason = "negative_day_km"
                    elif day_km > max_daily_km:
                        validation_status = "invalid"
                        validation_reason = "impossible_day_distance"

                    snapshot = existing_snapshots.get((device_id, snapshot_date))
                    if (
                        validation_status == "valid"
                        and snapshot
                        and snapshot.source in {"howen_daily_report", "manual_repair"}
                        and snapshot.day_km is not None
                        and abs(float(snapshot.day_km) - day_km) > tolerance
                        and snapshot.source == "manual_repair"
                    ):
                        validation_status = "review"
                        validation_reason = "source_disagreement"

                    observation_key = hashlib.sha256(
                        f"howen_daily_report|{company_slug}|{device_id}|{snapshot_date.isoformat()}".encode("utf-8")
                    ).hexdigest()
                    observation = existing_observations.get(observation_key)
                    if not observation:
                        observation = MileageObservation(
                            observation_key=observation_key,
                            company_slug=company_slug,
                            device_id=device_id,
                            observed_at=observed_at,
                            received_at=now_utc,
                            source="howen_daily_report",
                        )
                    observation.fleet_id = fleet_id
                    observation.plate_no = plate_no
                    observation.total_km = None
                    observation.day_km = day_km
                    observation.raw_total_value = None
                    observation.raw_day_value = str(raw_value)
                    observation.validation_status = validation_status
                    observation.validation_reason = validation_reason
                    observation.payload_json = json.dumps(
                        {
                            "device_id": device_id,
                            "date": snapshot_date.isoformat(),
                            "day_km": day_km,
                            "provider_total_mileage": source_row.get("totalMileage"),
                        },
                        ensure_ascii=True,
                    )
                    session.add(observation)
                    result.observations_written += 1

                    if validation_status != "valid":
                        if self._ensure_km_review(
                            session,
                            company_slug=company_slug,
                            device_id=device_id,
                            plate_no=plate_no,
                            snapshot_date=snapshot_date,
                            reason=validation_reason or "invalid_day_km",
                            diagnostic_note=_km_review_note(validation_reason),
                            payload={
                                "source": "howen_daily_report",
                                "proposed_day_km": day_km,
                                "previous_day_km": snapshot.day_km if snapshot else None,
                            },
                        ):
                            result.review_cases += 1
                        continue

                    if snapshot and snapshot.excluded_at is not None:
                        continue
                    if snapshot and snapshot.source == "manual_repair":
                        result.valid_device_days += 1
                        continue

                    total_km = (
                        device.last_total_km
                        if device and device.last_total_km is not None
                        else snapshot.total_km if snapshot else day_km
                    )
                    if not snapshot:
                        snapshot = DailyMileageSnapshot(
                            device_id=device_id,
                            snapshot_date=snapshot_date,
                            observed_at=observed_at,
                            total_km=float(total_km),
                        )
                        existing_snapshots[(device_id, snapshot_date)] = snapshot
                    elif snapshot.closure_status == "closed" and snapshot.day_km != day_km:
                        snapshot.late_updated_at = now_utc
                    snapshot.plate_no = plate_no or snapshot.plate_no
                    snapshot.company_slug = company_slug
                    snapshot.fleet_id = fleet_id or snapshot.fleet_id
                    snapshot.day_km = day_km
                    snapshot.total_km = float(total_km)
                    snapshot.raw_day_value = str(raw_value)
                    snapshot.km_validation_status = "valid"
                    snapshot.km_validation_reason = None
                    snapshot.source = "howen_daily_report"
                    snapshot.coverage_status = "covered"
                    snapshot.closure_status = "provisional" if snapshot_date == today_local else "closed"
                    snapshot.sample_count = max(snapshot.sample_count or 0, 1)
                    snapshot.first_observed_at = snapshot.first_observed_at or observed_at
                    snapshot.last_observed_at = observed_at
                    snapshot.observed_at = observed_at
                    snapshot.closed_at = None if snapshot_date == today_local else (snapshot.closed_at or now_utc)
                    session.add(snapshot)
                    result.snapshots_written += 1
                    result.valid_device_days += 1

            result.coverage_pct = round(
                (result.valid_device_days / result.expected_device_days * 100.0)
                if result.expected_device_days
                else 100.0,
                2,
            )
            approved_degraded = False
            if rebuild_job_id is not None:
                rebuild_job = session.get(CompanyHistoricalRebuildJob, rebuild_job_id)
                if rebuild_job:
                    approved_degraded = bool(rebuild_job.degraded_publication_approved)
                    rebuild_job.mileage_days_done = len(all_dates)
                    rebuild_job.mileage_devices_done = len(device_ids)
                    rebuild_job.mileage_rows_processed = result.expected_device_days
                    rebuild_job.mileage_valid_days = result.valid_device_days
                    rebuild_job.mileage_missing_days = result.missing_device_days
                    rebuild_job.mileage_coverage_pct = result.coverage_pct
                    rebuild_job.last_mileage_date = effective_end_date
                    rebuild_job.current_device_offset = len(device_ids)
                    rebuild_job.last_heartbeat_at = now_utc
                    session.add(rebuild_job)
            result.publication_allowed = bool(
                result.coverage_pct >= float(self.settings.mileage_rebuild_min_coverage_pct)
                or approved_degraded
            )
            session.commit()
        return result

    def _set_rebuild_phase(self, rebuild_job_id: int, *, phase: str, **values: Any) -> None:
        with self.session_factory() as session:
            rebuild_job = session.get(CompanyHistoricalRebuildJob, rebuild_job_id)
            if not rebuild_job:
                return
            rebuild_job.phase = phase
            rebuild_job.last_heartbeat_at = utc_now()
            for key, value in values.items():
                if hasattr(rebuild_job, key):
                    setattr(rebuild_job, key, value)
            session.add(rebuild_job)
            session.commit()
        self.mark_dirty()

    def _ensure_km_review(
        self,
        session: Any,
        *,
        company_slug: str,
        device_id: str,
        plate_no: str | None,
        snapshot_date: date,
        reason: str,
        diagnostic_note: str,
        payload: dict[str, Any],
    ) -> bool:
        review_key = f"km:{device_id}:{snapshot_date.isoformat()}:{reason}"
        existing = session.scalar(
            select(ReconciliationReview).where(ReconciliationReview.review_key == review_key)
        )
        if existing:
            return False
        observed_at = datetime.combine(
            snapshot_date,
            time(hour=12),
            ZoneInfo(self.registry.get(company_slug).timezone or self.settings.default_timezone),
        ).astimezone(ZoneInfo("UTC"))
        session.add(
            ReconciliationReview(
                review_key=review_key,
                company_slug=company_slug,
                device_id=device_id,
                plate_no=plate_no,
                observed_at=observed_at,
                portal_begin_time=snapshot_date.isoformat(),
                portal_reporting_time=observed_at.isoformat(),
                raw_alarm_type="Kilometraje",
                classification_status="km_review",
                visibility_status="km_pending_review",
                category="Kilometraje",
                subtype=reason,
                reason=reason,
                diagnostic_note=diagnostic_note,
                suggested_action="review_km",
                source_window_type="calendar_month_local",
                portal_payload_json=json.dumps(
                    {
                        "type": "km_review",
                        "company_slug": company_slug,
                        "device_id": device_id,
                        "plate_no": plate_no,
                        "issue_date": snapshot_date.isoformat(),
                        "reason": reason,
                        **payload,
                    },
                    ensure_ascii=True,
                ),
            )
        )
        return True

    async def rebuild_historical_window(
        self,
        request: HistoricalRebuildRequest,
        *,
        rebuild_job_id: int | None = None,
    ) -> dict[str, Any]:
        company = None
        company_tz = ZoneInfo(self.settings.default_timezone)
        start_date_local = request.start_date or utc_now().astimezone(company_tz).date()
        end_date_local = request.end_date or start_date_local
        device_ids: list[str] = []
        maintenance_enabled = False
        maintenance_reason = (
            f"historical_rebuild:{request.company_slug}:{start_date_local.isoformat()}:{end_date_local.isoformat()}"
        )
        day_results: list[dict[str, Any]] = []
        completed_days_offset = 0
        completed_days_total = 0
        total_inserted = 0
        total_anomalies = 0
        total_failed_count = 0
        batch_metrics = AlarmBatchResult()
        mileage_result: MileageRebuildResult | None = None
        latest_observed_at = None
        last_window_end_local = datetime.combine(end_date_local, time.max.replace(microsecond=0), company_tz)
        days_total = 0
        devices_total = 0
        chunk_days = max(int(getattr(self.settings, "historical_rebuild_chunk_days", 1) or 1), 1)

        try:
            company = self.registry.get(request.company_slug)
            if not self.registry.is_operational(company):
                raise ValueError("La empresa no esta operativa")

            start_date_local, end_date_local, company_tz = self._resolve_historical_rebuild_range(
                company_slug=request.company_slug,
                start_date_value=request.start_date,
                end_date_value=request.end_date,
                days=request.days,
            )
            maintenance_reason = (
                f"historical_rebuild:{request.company_slug}:{start_date_local.isoformat()}:{end_date_local.isoformat()}"
            )
            device_ids = self._list_company_device_ids(request.company_slug)
            last_window_end_local = datetime.combine(end_date_local, time.max.replace(microsecond=0), company_tz)
            days_total = (end_date_local - start_date_local).days + 1
            devices_total = len(device_ids)
            current_local_date = start_date_local

            if request.maintenance:
                await self.set_maintenance_mode(enabled=True, reason=maintenance_reason)
                maintenance_enabled = True
                drain_deadline = monotonic() + max(float(request.maintenance_drain_timeout), 0.0)
                while True:
                    with self.session_factory() as session:
                        running_harvests = session.scalar(
                            select(func.count())
                            .select_from(AlarmHarvestRun)
                            .where(
                                AlarmHarvestRun.company_slug == request.company_slug,
                                AlarmHarvestRun.status == "running",
                            )
                        ) or 0
                    if running_harvests <= 0 or monotonic() >= drain_deadline:
                        break
                    await asyncio.sleep(2.0)

            if rebuild_job_id is not None:
                with self.session_factory() as session:
                    rebuild_job = session.get(CompanyHistoricalRebuildJob, rebuild_job_id)
                    if rebuild_job:
                        if (
                            rebuild_job.status in {"queued", "running", "awaiting_approval"}
                            and rebuild_job.last_processed_date is not None
                            and start_date_local <= rebuild_job.last_processed_date <= end_date_local
                        ):
                            current_local_date = rebuild_job.last_processed_date + timedelta(days=1)
                            total_inserted = rebuild_job.inserted or 0
                            total_anomalies = rebuild_job.anomalies or 0
                            total_failed_count = rebuild_job.failed_count or 0
                            completed_days_offset = max(rebuild_job.days_done or 0, 0)
                            completed_days_total = completed_days_offset
                        rebuild_job.status = "running"
                        rebuild_job.phase = "alarm_provider"
                        rebuild_job.next_retry_at = None
                        rebuild_job.days_total = days_total
                        rebuild_job.devices_total = devices_total
                        rebuild_job.started_at = ensure_utc(rebuild_job.started_at) or utc_now()
                        rebuild_job.finished_at = None
                        rebuild_job.error_message = None
                        rebuild_job.last_heartbeat_at = utc_now()
                        session.add(rebuild_job)
                        session.commit()

            while current_local_date <= end_date_local:
                if rebuild_job_id is not None:
                    self._set_rebuild_phase(rebuild_job_id, phase="alarm_provider")
                chunk_end_date = min(
                    current_local_date + timedelta(days=chunk_days - 1),
                    end_date_local,
                )
                start_local = datetime.combine(current_local_date, time.min, company_tz)
                now_local = utc_now().astimezone(company_tz).replace(microsecond=0)
                if chunk_end_date >= now_local.date():
                    end_local = now_local
                else:
                    end_local = datetime.combine(chunk_end_date, time.max.replace(microsecond=0), company_tz)
                last_window_end_local = end_local
                result = await self._backfill_device_ids(
                    device_ids=device_ids,
                    start_at=start_local,
                    end_at=end_local,
                    source="harvest",
                    company_slug=request.company_slug,
                    rebuild_job_id=rebuild_job_id,
                    yield_to_live_harvest=True,
                    defer_on_rate_limit=True,
                    progress_callback=(
                        None
                        if rebuild_job_id is None
                        else lambda processed_devices, devices_total: self._update_rebuild_progress(
                            rebuild_job_id=rebuild_job_id,
                            days_total=days_total,
                            completed_days_offset=completed_days_total,
                            chunk_start_date=current_local_date,
                            chunk_end_date=chunk_end_date,
                            processed_devices=processed_devices,
                            devices_total=devices_total,
                        )
                    ),
                )
                processed_days = (chunk_end_date - current_local_date).days + 1
                total_inserted += int(result.get("inserted", 0))
                total_anomalies += int(result.get("anomalies", 0))
                total_failed_count += int(result.get("failed_count", 0))
                observed_at = parse_timestamp(result.get("latest_observed_at"))
                latest_observed_at = _max_datetime(latest_observed_at, observed_at)
                _merge_alarm_batch_metrics(batch_metrics, result.get("batch"))
                completed_days_total = min(days_total, completed_days_total + processed_days)
                day_results.append(
                    {
                        "start_date_local": current_local_date.isoformat(),
                        "end_date_local": chunk_end_date.isoformat(),
                        "processed_days": processed_days,
                        "inserted": int(result.get("inserted", 0)),
                        "anomalies": int(result.get("anomalies", 0)),
                        "failed_count": int(result.get("failed_count", 0)),
                        "latest_observed_at": result.get("latest_observed_at"),
                        "batch": result.get("batch"),
                    }
                )
                if rebuild_job_id is not None:
                    with self.session_factory() as session:
                        rebuild_job = session.get(CompanyHistoricalRebuildJob, rebuild_job_id)
                        if rebuild_job:
                            rebuild_job.days_done = min(days_total, completed_days_total)
                            rebuild_job.inserted = total_inserted
                            rebuild_job.anomalies = total_anomalies
                            rebuild_job.failed_count = total_failed_count
                            rebuild_job.last_processed_date = chunk_end_date
                            session.add(rebuild_job)
                            session.commit()
                current_local_date = chunk_end_date + timedelta(days=1)

            mileage_result = await self.rebuild_authoritative_mileage(
                company_slug=request.company_slug,
                start_date_local=start_date_local,
                end_date_local=end_date_local,
                company_tz=company_tz,
                device_ids=device_ids,
                rebuild_job_id=rebuild_job_id,
            )
            if not mileage_result.publication_allowed:
                if rebuild_job_id is not None:
                    with self.session_factory() as session:
                        rebuild_job = session.get(CompanyHistoricalRebuildJob, rebuild_job_id)
                        if rebuild_job:
                            rebuild_job.status = "awaiting_approval"
                            rebuild_job.phase = "awaiting_mileage_approval"
                            rebuild_job.error_message = (
                                "La cobertura de kilometraje no alcanza el minimo de publicacion. "
                                "Requiere aprobacion degradada del administrador."
                            )
                            rebuild_job.finished_at = utc_now()
                            rebuild_job.last_heartbeat_at = rebuild_job.finished_at
                            session.add(rebuild_job)
                            session.commit()
                    self.mark_dirty()
                return {
                    "status": "awaiting_approval",
                    "company_slug": request.company_slug,
                    "timezone": str(company_tz),
                    "start_date_local": start_date_local.isoformat(),
                    "end_date_local": end_date_local.isoformat(),
                    "days_total": days_total,
                    "devices_total": devices_total,
                    "inserted": total_inserted,
                    "anomalies": total_anomalies,
                    "failed_count": total_failed_count,
                    "mileage": mileage_result.as_dict(),
                    "message": "Reconstruccion completa, pendiente de aprobacion por cobertura de kilometraje.",
                }

            payload: dict[str, Any] | None = None
            if request.publish_snapshot:
                if rebuild_job_id is not None:
                    self._set_rebuild_phase(
                        rebuild_job_id,
                        phase="snapshot",
                        current_device_id=None,
                    )
                cut_at = self._resolve_safe_publish_cut_for_range(
                    company_slug=request.company_slug,
                    range_end_at=last_window_end_local,
                )
                payload = self.dashboard.materialize_snapshot(
                    request.company_slug,
                    cut_at=cut_at,
                    cut_status="succeeded",
                )
                self.mark_dirty()
                if rebuild_job_id is not None:
                    self._set_rebuild_phase(rebuild_job_id, phase="publication")
                    with self.session_factory() as session:
                        rebuild_job = session.get(CompanyHistoricalRebuildJob, rebuild_job_id)
                        if rebuild_job:
                            rebuild_job.published_cut_at = parse_timestamp(payload.get("meta", {}).get("publishedCutAt"))
                            session.add(rebuild_job)
                            session.commit()

            if rebuild_job_id is not None:
                with self.session_factory() as session:
                    rebuild_job = session.get(CompanyHistoricalRebuildJob, rebuild_job_id)
                    if rebuild_job:
                        rebuild_job.status = "succeeded"
                        rebuild_job.phase = "succeeded"
                        rebuild_job.days_done = days_total
                        rebuild_job.inserted = total_inserted
                        rebuild_job.anomalies = total_anomalies
                        rebuild_job.failed_count = total_failed_count
                        rebuild_job.last_processed_date = end_date_local
                        rebuild_job.finished_at = utc_now()
                        rebuild_job.current_device_id = None
                        rebuild_job.last_heartbeat_at = rebuild_job.finished_at
                        session.add(rebuild_job)
                        session.commit()

                # The selector/admin catalog must observe the terminal state, not
                # the earlier publishing state cached just before this commit.
                self.mark_dirty()

            return {
                "company_slug": request.company_slug,
                "timezone": str(company_tz),
                "start_date_local": start_date_local.isoformat(),
                "end_date_local": end_date_local.isoformat(),
                "days_total": days_total,
                "devices_total": devices_total,
                "inserted": total_inserted,
                "anomalies": total_anomalies,
                "failed_count": total_failed_count,
                "latest_observed_at": ensure_utc(latest_observed_at).isoformat() if latest_observed_at else None,
                "published_cut_at": payload.get("meta", {}).get("publishedCutAt") if payload else None,
                "recent_events": len(payload.get("recentEvents") or []) if payload else None,
                "week_total": payload.get("dms", {}).get("semana", {}).get("total") if payload else None,
                "last_dms_event_at": payload.get("meta", {}).get("lastDmsEventAt") if payload else None,
                "maintenance_mode": request.maintenance,
                "day_results": day_results,
                "batch": batch_metrics.as_dict() if batch_metrics.prepared_rows else None,
                "mileage": mileage_result.as_dict() if mileage_result else None,
            }
        except HistoricalBackfillDeferred as exc:
            if rebuild_job_id is not None:
                with self.session_factory() as session:
                    rebuild_job = session.get(CompanyHistoricalRebuildJob, rebuild_job_id)
                    if rebuild_job:
                        rebuild_job.status = "queued"
                        rebuild_job.phase = "waiting_retry"
                        rebuild_job.days_done = min(days_total, completed_days_total)
                        rebuild_job.inserted = total_inserted
                        rebuild_job.anomalies = total_anomalies
                        rebuild_job.failed_count = total_failed_count
                        rebuild_job.error_message = str(exc)
                        rebuild_job.next_retry_at = ensure_utc(exc.next_retry_at)
                        rebuild_job.finished_at = None
                        rebuild_job.last_heartbeat_at = utc_now()
                        session.add(rebuild_job)
                        session.commit()
            self.mark_dirty()
            return {
                "company_slug": request.company_slug,
                "status": "queued",
                "next_retry_at": ensure_utc(exc.next_retry_at).isoformat(),
                "message": str(exc),
                "inserted": total_inserted,
                "anomalies": total_anomalies,
                "failed_count": total_failed_count,
            }
        except Exception as exc:
            if rebuild_job_id is not None:
                with self.session_factory() as session:
                    rebuild_job = session.get(CompanyHistoricalRebuildJob, rebuild_job_id)
                    if rebuild_job:
                        rebuild_job.status = "failed"
                        rebuild_job.phase = "failed"
                        rebuild_job.days_done = min(days_total, completed_days_total)
                        rebuild_job.inserted = total_inserted
                        rebuild_job.anomalies = total_anomalies
                        rebuild_job.failed_count = total_failed_count
                        rebuild_job.error_message = str(exc)
                        rebuild_job.next_retry_at = None
                        rebuild_job.finished_at = utc_now()
                        rebuild_job.current_device_id = None
                        rebuild_job.last_heartbeat_at = rebuild_job.finished_at
                        session.add(rebuild_job)
                        session.commit()
            raise
        finally:
            if maintenance_enabled:
                await self.set_maintenance_mode(enabled=False, reason=None)

    def queue_historical_rebuild(
        self,
        request: HistoricalRebuildRequest,
        *,
        spawn: bool = True,
        purpose: str = "activation_bootstrap",
    ) -> int:
        start_date_local, end_date_local, _ = self._resolve_historical_rebuild_range(
            company_slug=request.company_slug,
            start_date_value=request.start_date,
            end_date_value=request.end_date,
            days=request.days,
        )
        devices_total = len(self._list_company_device_ids(request.company_slug))
        with self.session_factory() as session:
            existing = session.scalars(
                select(CompanyHistoricalRebuildJob)
                .where(
                    CompanyHistoricalRebuildJob.company_slug == request.company_slug,
                    CompanyHistoricalRebuildJob.purpose == purpose,
                    CompanyHistoricalRebuildJob.status.in_(("queued", "running")),
                )
                .order_by(CompanyHistoricalRebuildJob.created_at.desc(), CompanyHistoricalRebuildJob.id.desc())
            ).first()
            if existing:
                next_retry_at = ensure_utc(existing.next_retry_at)
                if spawn and (not next_retry_at or next_retry_at <= utc_now()) and self._can_start_historical_rebuild():
                    resume_request = HistoricalRebuildRequest(
                        company_slug=existing.company_slug,
                        start_date=existing.start_date,
                        end_date=existing.end_date,
                        publish_snapshot=request.publish_snapshot,
                        maintenance=request.maintenance,
                    )
                    self._spawn_historical_rebuild_task(request=resume_request, rebuild_job_id=existing.id)
                return existing.id
            rebuild_job = CompanyHistoricalRebuildJob(
                company_slug=request.company_slug,
                purpose=purpose,
                status="queued",
                start_date=start_date_local,
                end_date=end_date_local,
                days_total=(end_date_local - start_date_local).days + 1,
                days_done=0,
                devices_total=devices_total,
            )
            session.add(rebuild_job)
            session.commit()
            session.refresh(rebuild_job)
            rebuild_job_id = rebuild_job.id
        if spawn and self._can_start_historical_rebuild():
            self._spawn_historical_rebuild_task(request=request, rebuild_job_id=rebuild_job_id)
        return rebuild_job_id

    def approve_degraded_company_publication(
        self,
        *,
        company_slug: str,
        approved_by: str,
    ) -> dict[str, Any]:
        self.registry.get(company_slug)
        with self.session_factory() as session:
            rebuild_job = session.scalars(
                select(CompanyHistoricalRebuildJob)
                .where(
                    CompanyHistoricalRebuildJob.company_slug == company_slug,
                    CompanyHistoricalRebuildJob.purpose == "activation_bootstrap",
                )
                .order_by(CompanyHistoricalRebuildJob.created_at.desc(), CompanyHistoricalRebuildJob.id.desc())
            ).first()
            if not rebuild_job or rebuild_job.status != "awaiting_approval":
                raise ValueError("La empresa no tiene una publicacion degradada pendiente")
            rebuild_job.degraded_publication_approved = True
            rebuild_job.degraded_publication_approved_at = utc_now()
            rebuild_job.degraded_publication_approved_by = approved_by
            rebuild_job.status = "queued"
            rebuild_job.phase = "queued_degraded_publication"
            rebuild_job.next_retry_at = None
            rebuild_job.finished_at = None
            session.add(rebuild_job)
            session.commit()
            request = HistoricalRebuildRequest(
                company_slug=company_slug,
                start_date=rebuild_job.start_date,
                end_date=rebuild_job.end_date,
                publish_snapshot=True,
                maintenance=False,
            )
            rebuild_job_id = rebuild_job.id
        self.mark_dirty()
        return {
            "rebuild_job_id": rebuild_job_id,
            "request": request.model_dump(mode="json"),
        }

    def backup_company_operational_data(
        self,
        *,
        company_slug: str,
        requested_by: str,
    ) -> dict[str, Any]:
        """Create a restorable JSONL backup before destructive deactivation."""
        company = self.registry.get(company_slug)
        device_ids = sorted({*self._list_company_device_ids(company_slug), *company.device_ids})
        fleet_ids = sorted({fleet_id for fleet_id in company.fleet_ids if fleet_id})
        requested_at = utc_now()
        backup_dir = self.settings.company_backup_dir
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup_path = backup_dir / (
            f"{company_slug}-{requested_at.strftime('%Y%m%dT%H%M%SZ')}.jsonl.gz"
        )

        with self.session_factory() as session:
            audit = CompanyLifecycleAudit(
                company_slug=company_slug,
                action="deactivate",
                status="backing_up",
                requested_by=requested_by,
                requested_at=requested_at,
            )
            session.add(audit)
            session.commit()
            session.refresh(audit)
            audit_id = audit.id

        model_filters: list[tuple[str, Any, Any]] = [
            ("managed_companies", ManagedCompany, ManagedCompany.slug == company_slug),
            ("user_accounts", UserAccount, UserAccount.company_slug == company_slug),
            ("devices", DeviceRecord, self._build_company_scope_filter(
                company_slug=company_slug,
                company_column=DeviceRecord.company_slug,
                device_column=DeviceRecord.device_id,
                fleet_column=DeviceRecord.fleet_id,
                device_ids=device_ids,
                fleet_ids=fleet_ids,
            )),
            ("howen_alarm_raw", HowenAlarmRaw, HowenAlarmRaw.company_slug == company_slug),
            ("alarm_events", AlarmEvent, AlarmEvent.company_slug == company_slug),
            ("alarm_event_audit", AlarmEventAudit, AlarmEventAudit.company_slug == company_slug),
            ("daily_mileage_snapshots", DailyMileageSnapshot, DailyMileageSnapshot.company_slug == company_slug),
            ("mileage_observations", MileageObservation, MileageObservation.company_slug == company_slug),
            ("ingestion_anomalies", IngestionAnomaly, IngestionAnomaly.company_slug == company_slug),
            ("reconciliation_reviews", ReconciliationReview, ReconciliationReview.company_slug == company_slug),
            ("report_assets", ReportAsset, ReportAsset.company_slug == company_slug),
            ("published_dashboard_snapshots", PublishedDashboardSnapshot, PublishedDashboardSnapshot.company_slug == company_slug),
        ]
        row_count = 0
        files_backup_path: str | None = None
        try:
            with gzip.open(backup_path, "wt", encoding="utf-8") as output:
                output.write(json.dumps({
                    "type": "manifest",
                    "company_slug": company_slug,
                    "company": company.model_dump(mode="json"),
                    "created_at": requested_at.isoformat(),
                }, ensure_ascii=True, default=str) + "\n")
                with self.session_factory() as session:
                    for table_name, model, condition in model_filters:
                        if condition is None:
                            continue
                        rows = session.scalars(
                            select(model).where(condition).execution_options(yield_per=500)
                        )
                        columns = tuple(model.__table__.columns)
                        for row in rows:
                            payload = {
                                column.name: getattr(row, column.name)
                                for column in columns
                            }
                            output.write(json.dumps({
                                "type": "row",
                                "table": table_name,
                                "data": payload,
                            }, ensure_ascii=True, default=str) + "\n")
                            row_count += 1
            os.chmod(backup_path, 0o600)
            company_upload_dir = self.settings.upload_dir / company_slug
            if company_upload_dir.exists():
                files_archive_base = backup_dir / (
                    f"{company_slug}-{requested_at.strftime('%Y%m%dT%H%M%SZ')}-files"
                )
                files_backup_path = shutil.make_archive(
                    str(files_archive_base),
                    "gztar",
                    root_dir=company_upload_dir,
                )
                os.chmod(files_backup_path, 0o600)
            digest = hashlib.sha256(backup_path.read_bytes()).hexdigest()
            with self.session_factory() as session:
                audit = session.get(CompanyLifecycleAudit, audit_id)
                if audit:
                    audit.status = "backed_up"
                    audit.backup_path = str(backup_path)
                    audit.backup_sha256 = digest
                    audit.row_count = row_count
                    audit.detail_json = json.dumps(
                        {"files_backup_path": files_backup_path},
                        ensure_ascii=True,
                    )
                    session.add(audit)
                    session.commit()
            return {
                "audit_id": audit_id,
                "backup_path": str(backup_path),
                "backup_sha256": digest,
                "row_count": row_count,
                "files_backup_path": files_backup_path,
            }
        except Exception as exc:
            with self.session_factory() as session:
                audit = session.get(CompanyLifecycleAudit, audit_id)
                if audit:
                    audit.status = "backup_failed"
                    audit.error_message = str(exc)
                    session.add(audit)
                    session.commit()
            raise

    def finish_company_lifecycle_audit(
        self,
        *,
        audit_id: int,
        status: str,
        detail: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        with self.session_factory() as session:
            audit = session.get(CompanyLifecycleAudit, audit_id)
            if not audit:
                return
            audit.status = status
            audit.completed_at = utc_now()
            audit.detail_json = json.dumps(detail or {}, ensure_ascii=True, default=str)
            audit.error_message = error
            session.add(audit)
            session.commit()

    async def rebuild_daily_mileage(
        self,
        *,
        company_slug: str,
        target_date: date,
    ) -> dict[str, Any]:
        company = self.registry.get(company_slug)
        company_tz = ZoneInfo(company.timezone or self.settings.default_timezone)
        result = await self.rebuild_authoritative_mileage(
            company_slug=company_slug,
            start_date_local=target_date,
            end_date_local=target_date,
            company_tz=company_tz,
            device_ids=self._list_company_device_ids(company_slug),
            rebuild_job_id=None,
        )
        self.dashboard.clear_runtime_caches()
        self.mark_dirty()
        return {
            "company_slug": company_slug,
            "target_date": target_date.isoformat(),
            **result.as_dict(),
        }

    async def rebuild_mileage_window(self, request: Any) -> dict[str, Any]:
        """Rebuild an existing company's mileage from Howen's daily report."""
        company = self.registry.get(request.company_slug)
        company_tz = ZoneInfo(company.timezone or self.settings.default_timezone)
        today_local = utc_now().astimezone(company_tz).date()
        start_date_local = request.start_date or (today_local - timedelta(days=29))
        end_date_local = request.end_date or today_local
        if start_date_local > end_date_local:
            raise ValueError("start_date debe ser menor o igual que end_date")

        result = await self.rebuild_authoritative_mileage(
            company_slug=company.slug,
            start_date_local=start_date_local,
            end_date_local=end_date_local,
            company_tz=company_tz,
            device_ids=self._list_company_device_ids(company.slug),
            rebuild_job_id=None,
        )
        self.dashboard.clear_runtime_caches()
        self.mark_dirty()
        return {
            "company_slug": company.slug,
            "start_date": start_date_local.isoformat(),
            "end_date": end_date_local.isoformat(),
            "source": "howen_daily_report",
            **result.as_dict(),
        }

    async def purge_company_operational_data(self, *, company_slug: str) -> dict[str, Any]:
        company = self.registry.get(company_slug)
        device_ids = sorted({*self._list_company_device_ids(company_slug), *company.device_ids})
        fleet_ids = sorted({fleet_id for fleet_id in company.fleet_ids if fleet_id})
        maintenance_enabled = False
        maintenance_reason = f"company_purge:{company_slug}"

        try:
            await self.set_maintenance_mode(enabled=True, reason=maintenance_reason)
            maintenance_enabled = True
            drain_deadline = monotonic() + 30.0
            while True:
                with self.session_factory() as session:
                    running_harvests = session.scalar(
                        select(func.count())
                        .select_from(AlarmHarvestRun)
                        .where(AlarmHarvestRun.status == "running")
                    ) or 0
                if running_harvests <= 0 or monotonic() >= drain_deadline:
                    break
                await asyncio.sleep(1.0)

            with self.session_factory() as session:
                harvest_run_ids = list(
                    session.scalars(select(AlarmHarvestRun.id).where(AlarmHarvestRun.company_slug == company_slug))
                )
                reconciliation_job_ids = list(
                    session.scalars(select(ReconciliationJob.id).where(ReconciliationJob.company_slug == company_slug))
                )

                deleted_counts = {
                    "alarm_harvest_devices": 0,
                    "alarm_harvest_runs": 0,
                    "historical_rebuild_jobs": 0,
                    "reconciliation_job_devices": 0,
                    "reconciliation_jobs": 0,
                    "reconciliation_reviews": 0,
                    "published_snapshots": 0,
                    "catchup_cursors": 0,
                    "report_assets": 0,
                    "daily_mileage_snapshots": 0,
                    "mileage_readings": 0,
                    "mileage_observations": 0,
                    "alarm_events": 0,
                    "alarm_event_audit": 0,
                    "howen_alarm_raw": 0,
                    "ingestion_anomalies": 0,
                }

                if harvest_run_ids:
                    deleted_counts["alarm_harvest_devices"] = session.query(AlarmHarvestDevice).filter(
                        AlarmHarvestDevice.run_id.in_(harvest_run_ids)
                    ).delete(synchronize_session=False)
                if reconciliation_job_ids:
                    deleted_counts["reconciliation_job_devices"] = session.query(ReconciliationJobDevice).filter(
                        ReconciliationJobDevice.job_id.in_(reconciliation_job_ids)
                    ).delete(synchronize_session=False)

                deleted_counts["reconciliation_reviews"] = session.query(ReconciliationReview).filter(
                    ReconciliationReview.company_slug == company_slug
                ).delete(synchronize_session=False)
                deleted_counts["published_snapshots"] = session.query(PublishedDashboardSnapshot).filter(
                    PublishedDashboardSnapshot.company_slug == company_slug
                ).delete(synchronize_session=False)
                deleted_counts["catchup_cursors"] = session.query(CatchupCursor).filter(
                    CatchupCursor.company_slug == company_slug
                ).delete(synchronize_session=False)
                deleted_counts["report_assets"] = session.query(ReportAsset).filter(
                    ReportAsset.company_slug == company_slug
                ).delete(synchronize_session=False)
                deleted_counts["alarm_harvest_runs"] = session.query(AlarmHarvestRun).filter(
                    AlarmHarvestRun.company_slug == company_slug
                ).delete(synchronize_session=False)
                deleted_counts["historical_rebuild_jobs"] = session.query(CompanyHistoricalRebuildJob).filter(
                    CompanyHistoricalRebuildJob.company_slug == company_slug
                ).delete(synchronize_session=False)
                deleted_counts["reconciliation_jobs"] = session.query(ReconciliationJob).filter(
                    ReconciliationJob.company_slug == company_slug
                ).delete(synchronize_session=False)

                snapshot_filter = self._build_company_scope_filter(
                    company_slug=company_slug,
                    company_column=DailyMileageSnapshot.company_slug,
                    device_column=DailyMileageSnapshot.device_id,
                    fleet_column=DailyMileageSnapshot.fleet_id,
                    device_ids=device_ids,
                    fleet_ids=fleet_ids,
                )
                if snapshot_filter is not None:
                    deleted_counts["daily_mileage_snapshots"] = session.query(DailyMileageSnapshot).filter(
                        snapshot_filter
                    ).delete(synchronize_session=False)

                mileage_filter = self._build_company_scope_filter(
                    company_slug=company_slug,
                    device_column=MileageReading.device_id,
                    fleet_column=MileageReading.fleet_id,
                    device_ids=device_ids,
                    fleet_ids=fleet_ids,
                )
                if mileage_filter is not None:
                    deleted_counts["mileage_readings"] = session.query(MileageReading).filter(
                        mileage_filter
                    ).delete(synchronize_session=False)
                deleted_counts["mileage_observations"] = session.query(MileageObservation).filter(
                    MileageObservation.company_slug == company_slug
                ).delete(synchronize_session=False)

                alarm_filter = self._build_company_scope_filter(
                    company_slug=company_slug,
                    company_column=AlarmEvent.company_slug,
                    device_column=AlarmEvent.device_id,
                    fleet_column=AlarmEvent.fleet_id,
                    device_ids=device_ids,
                    fleet_ids=fleet_ids,
                )
                if alarm_filter is not None:
                    deleted_counts["alarm_events"] = session.query(AlarmEvent).filter(alarm_filter).delete(
                        synchronize_session=False
                    )

                audit_filter = self._build_company_scope_filter(
                    company_slug=company_slug,
                    company_column=AlarmEventAudit.company_slug,
                    device_column=AlarmEventAudit.device_id,
                    fleet_column=AlarmEventAudit.fleet_id,
                    device_ids=device_ids,
                    fleet_ids=fleet_ids,
                )
                if audit_filter is not None:
                    deleted_counts["alarm_event_audit"] = session.query(AlarmEventAudit).filter(audit_filter).delete(
                        synchronize_session=False
                    )

                raw_filter = self._build_company_scope_filter(
                    company_slug=company_slug,
                    company_column=HowenAlarmRaw.company_slug,
                    device_column=HowenAlarmRaw.device_id,
                    fleet_column=HowenAlarmRaw.fleet_id,
                    device_ids=device_ids,
                    fleet_ids=fleet_ids,
                )
                if raw_filter is not None:
                    deleted_counts["howen_alarm_raw"] = session.query(HowenAlarmRaw).filter(raw_filter).delete(
                        synchronize_session=False
                    )

                anomaly_filter = self._build_company_scope_filter(
                    company_slug=company_slug,
                    company_column=IngestionAnomaly.company_slug,
                    device_column=IngestionAnomaly.device_id,
                    device_ids=device_ids,
                )
                if anomaly_filter is not None:
                    deleted_counts["ingestion_anomalies"] = session.query(IngestionAnomaly).filter(anomaly_filter).delete(
                        synchronize_session=False
                    )

                session.commit()
        finally:
            if maintenance_enabled:
                await self.set_maintenance_mode(enabled=False, reason=None)

        with suppress(FileNotFoundError):
            shutil.rmtree(self.settings.upload_dir / company_slug)
        self.dashboard.clear_runtime_caches()
        return {
            "company_slug": company_slug,
            "fleet_ids": len(fleet_ids),
            "device_ids": len(device_ids),
            **deleted_counts,
        }

    @staticmethod
    def _build_company_scope_filter(
        *,
        company_slug: str,
        company_column: Any | None = None,
        device_column: Any | None = None,
        fleet_column: Any | None = None,
        device_ids: list[str] | None = None,
        fleet_ids: list[str] | None = None,
    ) -> Any | None:
        conditions: list[Any] = []
        if company_column is not None:
            conditions.append(company_column == company_slug)
        if device_column is not None and device_ids:
            conditions.append(device_column.in_(device_ids))
        if fleet_column is not None and fleet_ids:
            conditions.append(fleet_column.in_(fleet_ids))
        if not conditions:
            return None
        if len(conditions) == 1:
            return conditions[0]
        return or_(*conditions)

    async def _run_due_harvests(self) -> None:
        if self._maintenance_active():
            return
        self._cleanup_orphan_harvest_runs(include_current_cut=False)
        latest_due_cut = self._latest_due_cut()

        for company in self.registry.all():
            if not self.registry.is_operational(company):
                continue
            if self._activation_bootstrap_running(company.slug):
                continue
            lock = self._harvest_locks.setdefault(company.slug, asyncio.Lock())
            if lock.locked():
                continue
            pending_cuts: list[datetime] = []
            with self.session_factory() as session:
                publication = session.get(PublishedDashboardSnapshot, company.slug)
                last_cut = ensure_utc(publication.published_cut_at) if publication and publication.published_cut_at else None
            if last_cut is None:
                pending_cuts.append(latest_due_cut)
            elif last_cut < latest_due_cut:
                pending_cuts.append(latest_due_cut)
            for cut_at in pending_cuts:
                await self._run_harvest_for_cut(company_slug=company.slug, cut_at=cut_at, force=False)

    async def _run_harvest_for_cut(self, *, company_slug: str, cut_at: datetime, force: bool) -> dict[str, Any]:
        company = self.registry.get(company_slug)
        if not self.registry.is_operational(company):
            return {
                "company_slug": company_slug,
                "cut_at": (ensure_utc(cut_at) or utc_now()).isoformat(),
                "status": "skipped",
                "error_message": "Company is not operationally configured",
            }
        if not force and self._activation_bootstrap_running(company.slug):
            return {
                "company_slug": company_slug,
                "cut_at": (ensure_utc(cut_at) or utc_now()).isoformat(),
                "status": "bootstrap_running",
                "error_message": "Company activation bootstrap is still rebuilding historical data",
            }
        if self._maintenance_active() and not force:
            return {
                "company_slug": company_slug,
                "cut_at": (ensure_utc(cut_at) or utc_now()).isoformat(),
                "status": "maintenance",
                "error_message": "Historical harvest is paused by maintenance mode",
            }

        cut_at = ensure_utc(cut_at) or utc_now()
        window_start, window_end = self._harvest_window_for_cut(
            cut_at,
            company_slug=company.slug,
        )
        device_ids = self._list_company_device_ids(company_slug)
        lock = self._harvest_locks.setdefault(company.slug, asyncio.Lock())
        async with lock:
            run_id: int | None = None
            with self.session_factory() as session:
                stale_runs = list(
                    session.scalars(
                        select(AlarmHarvestRun).where(
                            AlarmHarvestRun.company_slug == company.slug,
                            AlarmHarvestRun.status == "running",
                            AlarmHarvestRun.cut_at != cut_at,
                        )
                    )
                )
                for stale_run in stale_runs:
                    stale_run.status = "failed"
                    stale_run.finished_at = utc_now()
                    stale_run.error_message = stale_run.error_message or "Superseded by a newer harvest run"
                    session.add(stale_run)
                run = session.scalar(
                    select(AlarmHarvestRun).where(
                        AlarmHarvestRun.company_slug == company.slug,
                        AlarmHarvestRun.cut_at == cut_at,
                    )
                )
                if run and run.status == "succeeded" and not force:
                    published = session.get(PublishedDashboardSnapshot, company.slug)
                    if published and ensure_utc(published.published_cut_at) and ensure_utc(published.published_cut_at) >= cut_at:
                        return self._serialize_harvest_run(run.id)
                if not run:
                    run = AlarmHarvestRun(
                        company_slug=company.slug,
                        cut_at=cut_at,
                        window_start=window_start,
                        window_end=window_end,
                    )
                    session.add(run)
                    session.flush()
                first_attempt = run.started_at is None
                run.window_start = window_start
                run.window_end = window_end
                run.status = "running"
                run.devices_total = len(device_ids)
                run.started_at = run.started_at or utc_now()
                run.finished_at = None
                run.error_message = None

                existing_devices = {
                    row.device_id: row
                    for row in session.scalars(
                        select(AlarmHarvestDevice).where(AlarmHarvestDevice.run_id == run.id)
                    )
                }
                completed_devices: set[str] = set()
                for device_id in device_ids:
                    device_row = existing_devices.get(device_id) or AlarmHarvestDevice(run_id=run.id, device_id=device_id)
                    if not force and device_row.status == "succeeded":
                        completed_devices.add(device_id)
                        continue
                    device_row.status = "queued"
                    device_row.provider_rows = 0
                    device_row.provider_dms_rows = 0
                    device_row.inserted_raw = 0
                    device_row.inserted_dms = 0
                    device_row.future_rejected = 0
                    device_row.error_message = None
                    device_row.started_at = None
                    device_row.finished_at = None
                    session.add(device_row)
                completed_rows = [
                    row for device_id, row in existing_devices.items() if device_id in completed_devices
                ]
                run.devices_done = len(completed_devices)
                run.rows_total = sum(int(row.provider_rows or 0) for row in completed_rows)
                run.dms_total = sum(int(row.provider_dms_rows or 0) for row in completed_rows)
                session.commit()
                run_id = run.id
                pending_device_ids = [device_id for device_id in device_ids if device_id not in completed_devices]

            if not first_attempt and pending_device_ids:
                logger.info(
                    "harvest_resume company=%s cut=%s completed=%s pending=%s",
                    company.slug,
                    cut_at.isoformat(),
                    len(device_ids) - len(pending_device_ids),
                    len(pending_device_ids),
                )

            await self._set_publication_state(
                company_slug=company.slug,
                cut_at=cut_at,
                status="running",
                last_error=None,
            )

            run_status = "succeeded"
            any_failed = False
            with self.session_factory() as session:
                current_run = session.get(AlarmHarvestRun, run_id)
                rows_total = int(current_run.rows_total or 0) if current_run else 0
                dms_total = int(current_run.dms_total or 0) if current_run else 0
            next_retry_at: datetime | None = None
            evidence_rows_by_device: dict[str, list[dict[str, Any]]] | None = None
            use_device_api_fallback = False

            for device_id in pending_device_ids:
                with self.session_factory() as session:
                    device_row = session.scalar(
                        select(AlarmHarvestDevice).where(
                            AlarmHarvestDevice.run_id == run_id,
                            AlarmHarvestDevice.device_id == device_id,
                        )
                    )
                    record = session.get(DeviceRecord, device_id)
                    if device_row:
                        device_row.plate_no = record.plate_no if record else device_row.plate_no
                        device_row.status = "running"
                        device_row.started_at = utc_now()
                        session.add(device_row)
                        session.commit()
                try:
                    if (
                        str(getattr(self.settings, "howen_alarm_source", "evidence_bulk")) == "evidence_bulk"
                        and not use_device_api_fallback
                    ):
                        if evidence_rows_by_device is None:
                            try:
                                evidence_rows_by_device = await self._fetch_evidence_harvest_rows(
                                    device_ids=pending_device_ids,
                                    start_at=window_start,
                                    end_at=window_end,
                                )
                                logger.info(
                                    "harvest_evidence_bulk company=%s cut=%s devices=%s rows=%s",
                                    company.slug,
                                    cut_at.isoformat(),
                                    len(pending_device_ids),
                                    sum(len(items) for items in evidence_rows_by_device.values()),
                                )
                            except Exception as exc:
                                if self.howen.is_rate_limited(exc):
                                    raise
                                if not bool(
                                    getattr(self.settings, "howen_evidence_fallback_to_device_api", True)
                                ):
                                    raise
                                logger.warning(
                                    "harvest_evidence_fallback company=%s cut=%s error=%s",
                                    company.slug,
                                    cut_at.isoformat(),
                                    exc,
                                )
                                use_device_api_fallback = True
                        if not use_device_api_fallback:
                            rows = evidence_rows_by_device.get(device_id, [])
                        else:
                            rows = await self._fetch_historical_backfill_rows(
                                device_id=device_id,
                                start_at=window_start,
                                end_at=window_end,
                                source="harvest",
                                defer_on_rate_limit=True,
                            )
                    else:
                        rows = await self._fetch_historical_backfill_rows(
                            device_id=device_id,
                            start_at=window_start,
                            end_at=window_end,
                            source="harvest",
                            defer_on_rate_limit=True,
                        )
                except HistoricalBackfillDeferred as exc:
                    run_status = "rate_limited"
                    next_retry_at = ensure_utc(exc.next_retry_at)
                    with self.session_factory() as session:
                        device_row = session.scalar(
                            select(AlarmHarvestDevice).where(
                                AlarmHarvestDevice.run_id == run_id,
                                AlarmHarvestDevice.device_id == device_id,
                            )
                        )
                        if device_row:
                            device_row.status = "rate_limited"
                            device_row.error_message = str(exc)
                            device_row.finished_at = utc_now()
                            session.add(device_row)
                        run_row = session.get(AlarmHarvestRun, run_id)
                        if run_row:
                            run_row.error_message = str(exc)
                            session.add(run_row)
                        session.commit()
                    break
                except Exception as exc:
                    status_label = "rate_limited" if self.howen.is_rate_limited(exc) else "failed"
                    if status_label == "rate_limited":
                        run_status = "rate_limited"
                    else:
                        any_failed = True
                        if run_status != "rate_limited":
                            run_status = "partial"
                    with self.session_factory() as session:
                        device_row = session.scalar(
                            select(AlarmHarvestDevice).where(
                                AlarmHarvestDevice.run_id == run_id,
                                AlarmHarvestDevice.device_id == device_id,
                            )
                        )
                        if device_row:
                            device_row.status = status_label
                            device_row.error_message = str(exc)
                            device_row.finished_at = utc_now()
                            session.add(device_row)
                        run_row = session.get(AlarmHarvestRun, run_id)
                        if run_row:
                            run_row.devices_done += 1
                            run_row.error_message = str(exc)
                            session.add(run_row)
                        session.commit()
                    if status_label == "rate_limited":
                        break
                    continue

                provider_dms_rows = 0
                inserted_raw = 0
                inserted_dms = 0
                future_rejected = 0
                for row in rows:
                    if not isinstance(row, dict):
                        await self._record_normalization_failure(
                            source_type="harvest_alarm",
                            payload={"device_id": device_id, "raw_row": row},
                            received_at=utc_now(),
                        )
                        continue
                    alarm = self.howen.normalize_alarm(row)
                    received_at = utc_now()
                    if not alarm:
                        if self.howen.is_ignorable_historical_alarm(row):
                            continue
                        await self._record_normalization_failure(
                            source_type="harvest_alarm",
                            payload=row,
                            received_at=received_at,
                        )
                        continue
                    if alarm.classification_status == "classified_dms":
                        provider_dms_rows += 1
                    ingest_result = await self.ingest_alarm(alarm, received_at=received_at, source="harvest")
                    if ingest_result["inserted_raw"]:
                        inserted_raw += 1
                    if ingest_result["inserted_alarm_event"]:
                        inserted_dms += 1
                    if ingest_result["temporal_status"] == "future_rejected":
                        future_rejected += 1

                rows_total += len(rows)
                dms_total += provider_dms_rows
                with self.session_factory() as session:
                    device_row = session.scalar(
                        select(AlarmHarvestDevice).where(
                            AlarmHarvestDevice.run_id == run_id,
                            AlarmHarvestDevice.device_id == device_id,
                        )
                    )
                    if device_row:
                        device_row.status = "succeeded"
                        device_row.provider_rows = len(rows)
                        device_row.provider_dms_rows = provider_dms_rows
                        device_row.inserted_raw = inserted_raw
                        device_row.inserted_dms = inserted_dms
                        device_row.future_rejected = future_rejected
                        device_row.finished_at = utc_now()
                        session.add(device_row)
                    run_row = session.get(AlarmHarvestRun, run_id)
                    if run_row:
                        run_row.devices_done += 1
                        run_row.rows_total = rows_total
                        run_row.dms_total = dms_total
                        session.add(run_row)
                    session.commit()

            with self.session_factory() as session:
                run_row = session.get(AlarmHarvestRun, run_id)
                if run_row:
                    run_row.rows_total = rows_total
                    run_row.dms_total = dms_total
                    run_row.finished_at = utc_now()
                    run_row.status = run_status if run_status != "succeeded" else ("partial" if any_failed else "succeeded")
                    session.add(run_row)
                    session.commit()
                    run_status = run_row.status

            cohort_result: dict[str, Any] = {}
            if run_status == "succeeded" and not force:
                cohort_result = await self._publish_harvest_cohort_if_ready(cut_at=cut_at)
            elif run_status == "succeeded":
                publish_cut_at = self._resolve_safe_publish_cut_for_harvest(
                    company_slug=company.slug,
                    harvested_cut_at=cut_at,
                )
                payload = await asyncio.to_thread(
                    self.dashboard.materialize_snapshot,
                    company.slug,
                    cut_at=publish_cut_at,
                    cut_status="succeeded",
                )
                await self.hub.publish(company.slug, payload)
                self.mark_dirty()
            else:
                await self._set_publication_state(
                    company_slug=company.slug,
                    cut_at=cut_at,
                    status=run_status,
                    last_error=self._harvest_run_error(run_id),
                )
            serialized = self._serialize_harvest_run(run_id)
            serialized.update(cohort_result)
            if next_retry_at is not None:
                serialized["next_retry_at"] = next_retry_at.isoformat()
            return serialized

    async def _fetch_evidence_harvest_rows(
        self,
        *,
        device_ids: list[str],
        start_at: datetime,
        end_at: datetime,
    ) -> dict[str, list[dict[str, Any]]]:
        if not device_ids:
            return {}
        account_device_ids = sorted(
            {
                device_id
                for company in self.registry.all()
                if self.registry.is_operational(company)
                for device_id in self._list_company_device_ids(company.slug)
            }
        )
        if not account_device_ids:
            account_device_ids = list(device_ids)
        local_start_at, local_end_at = self._historical_window_for_device(
            device_id=account_device_ids[0],
            start_at=start_at,
            end_at=end_at,
        )
        cache_key = (local_start_at.isoformat(), local_end_at.isoformat())
        fetch_task = self._evidence_fetch_tasks.get(cache_key)
        if fetch_task is None:
            fetch_task = asyncio.create_task(
                self.howen.fetch_evidence_alarms_authorized(
                    device_ids=account_device_ids,
                    start_at=local_start_at,
                    end_at=local_end_at,
                    force_login=False,
                ),
                name=f"howen-evidence-{local_end_at.isoformat()}",
            )
            self._evidence_fetch_tasks[cache_key] = fetch_task
            logger.info(
                "harvest_evidence_account_fetch window_end=%s devices=%s",
                local_end_at.isoformat(),
                len(account_device_ids),
            )
            completed_keys = [
                key
                for key, task in self._evidence_fetch_tasks.items()
                if key != cache_key and task.done()
            ]
            for stale_key in completed_keys[:-7]:
                self._evidence_fetch_tasks.pop(stale_key, None)
        try:
            rows = await asyncio.shield(fetch_task)
        except Exception:
            if self._evidence_fetch_tasks.get(cache_key) is fetch_task:
                self._evidence_fetch_tasks.pop(cache_key, None)
            raise
        expected_ids = set(device_ids)
        grouped: dict[str, list[dict[str, Any]]] = {device_id: [] for device_id in device_ids}
        for row in rows:
            device_id = str(
                row.get("deviceID") or row.get("deviceno") or row.get("deviceid") or ""
            ).strip()
            if device_id in expected_ids:
                grouped[device_id].append(row)
                continue
            await self._record_normalization_failure(
                source_type="harvest_evidence_device",
                payload=row,
                received_at=utc_now(),
            )
        return grouped

    async def _run_live_forever(self) -> None:
        force_login = False
        while True:
            retry_delay = 15
            try:
                await self._set_state(connection_state="connecting", last_error=None)
                session = await self.howen.resolve_session(force_login=force_login)
                await self.sync_devices(force=True)
                await self._set_state(connection_state="connected", last_error=None)
                if self._harvest_scheduler_enabled:
                    asyncio.create_task(self._run_due_harvests(), name="dashboard-harvest-kickoff")
                force_login = False

                async for message in self.howen.listen(session):
                    if not isinstance(message, dict):
                        await self._record_ws_message_anomaly(
                            action="__invalid_message__",
                            payload=message,
                            received_at=utc_now(),
                        )
                        continue
                    action = str(message.get("action") or "")
                    received_at = utc_now()
                    raw_payload = message.get("payload")
                    if action in {"__raw_text__", "__non_dict__"}:
                        await self._record_ws_message_anomaly(
                            action=action,
                            payload=raw_payload,
                            received_at=received_at,
                        )
                        continue

                    if action in {"80003", "80004"} and not isinstance(raw_payload, dict):
                        await self._record_ws_message_anomaly(
                            action=action,
                            payload=raw_payload,
                            received_at=received_at,
                        )
                        continue

                    payload = raw_payload if isinstance(raw_payload, dict) else {}
                    payload_text = None if isinstance(raw_payload, dict) or raw_payload in (None, "") else str(raw_payload)

                    if action == "80003":
                        status = self.howen.normalize_status(payload)
                        if not status:
                            await self._record_normalization_failure(
                                source_type="status",
                                payload=payload,
                                received_at=received_at,
                            )
                        elif await self._validate_temporal_integrity(
                            source_type="status",
                            device_id=status.device_id,
                            fleet_id=status.fleet_id,
                            observed_at=status.observed_at,
                            raw_event_time=status.raw_event_time,
                            received_at=received_at,
                            payload=status.raw,
                        ):
                            await self.ingest_status(status, received_at=received_at)
                    elif action == "80004":
                        alarm = self.howen.normalize_alarm(payload)
                        if not alarm:
                            await self._record_normalization_failure(
                                source_type="alarm",
                                payload=payload,
                                received_at=received_at,
                            )
                        else:
                            await self._capture_live_alarm_telemetry(alarm=alarm, received_at=received_at)
                    elif action == "80000" and ((payload.get("result") or "").lower() == "fail" or (payload_text or "").lower() == "fail"):
                        raise RuntimeError(payload.get("msg") or payload_text or "Howen websocket login failed")
                    elif action == "80009" and self.howen.is_auth_error(payload.get("msg") or payload.get("result") or payload_text or ""):
                        raise RuntimeError(payload.get("msg") or payload_text or "Howen heartbeat rejected the current session")

                    if self._should_sync_devices():
                        await self.sync_devices(force=False)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.howen.is_auth_error(exc):
                    self.howen.clear_cached_session()
                    force_login = True
                    retry_delay = 10
                elif self.howen.is_login_rate_limited(exc):
                    force_login = True
                    retry_delay = 60
                else:
                    retry_delay = 15
                await self._set_state(connection_state="reconnecting", last_error=str(exc))
                await asyncio.sleep(retry_delay)

    async def _capture_live_alarm_telemetry(self, *, alarm: NormalizedAlarm, received_at: datetime) -> None:
        received_at = ensure_utc(received_at) or utc_now()
        observed_at = ensure_utc(alarm.occurred_at) or alarm.occurred_at
        with self.session_factory() as session:
            state = session.get(IngestState, "global")
            if not state:
                state = IngestState(key="global")
                session.add(state)
            state.mode = "live"
            state.connection_state = "connected"
            state.last_message_at = _max_datetime(state.last_message_at, received_at)
            state.last_live_alarm_message_at = _max_datetime(state.last_live_alarm_message_at, received_at)
            state.last_error = None
            if observed_at is not None:
                state.last_alarm_at = _max_datetime(state.last_alarm_at, observed_at)
                if alarm.classification_status == "classified_dms":
                    state.last_live_dms_at = _max_datetime(state.last_live_dms_at, observed_at)
                elif alarm.classification_status == "unmapped":
                    state.last_live_unmapped_at = _max_datetime(state.last_live_unmapped_at, observed_at)
            session.commit()

    async def _set_publication_state(
        self,
        *,
        company_slug: str,
        cut_at: datetime,
        status: str,
        last_error: str | None,
    ) -> None:
        cut_at = ensure_utc(cut_at) or utc_now()
        next_cut_at = cut_at + self._harvest_interval()
        with self.session_factory() as session:
            ingest_state = session.get(IngestState, "global")
            publication = session.get(PublishedDashboardSnapshot, company_slug) or PublishedDashboardSnapshot(company_slug=company_slug)
            published_cut = ensure_utc(publication.published_cut_at) if publication.published_cut_at else None
            if published_cut is not None and published_cut > cut_at:
                return
            publication.next_cut_at = next_cut_at
            publication.window_start = cut_at - self._harvest_interval()
            publication.window_end = cut_at
            publication.cut_status = status
            publication.last_error = last_error
            publication.last_status_message_at = ensure_utc(ingest_state.last_message_at) if ingest_state else publication.last_status_message_at
            publication.last_status_observed_at = ensure_utc(ingest_state.last_status_at) if ingest_state else publication.last_status_observed_at
            session.add(publication)
            session.commit()
        self.dashboard.clear_runtime_caches()

    def _harvest_run_error(self, run_id: int) -> str | None:
        with self.session_factory() as session:
            run = session.get(AlarmHarvestRun, run_id)
            return run.error_message if run else None

    def _serialize_harvest_run(self, run_id: int) -> dict[str, Any]:
        with self.session_factory() as session:
            run = session.get(AlarmHarvestRun, run_id)
            if not run:
                return {"status": "missing", "run_id": run_id}
            devices = list(session.scalars(select(AlarmHarvestDevice).where(AlarmHarvestDevice.run_id == run_id)))
        return {
            "run_id": run.id,
            "company_slug": run.company_slug,
            "cut_at": ensure_utc(run.cut_at).isoformat() if run.cut_at else None,
            "window_start": ensure_utc(run.window_start).isoformat() if run.window_start else None,
            "window_end": ensure_utc(run.window_end).isoformat() if run.window_end else None,
            "status": run.status,
            "devices_total": run.devices_total,
            "devices_done": run.devices_done,
            "rows_total": run.rows_total,
            "dms_total": run.dms_total,
            "started_at": ensure_utc(run.started_at).isoformat() if run.started_at else None,
            "finished_at": ensure_utc(run.finished_at).isoformat() if run.finished_at else None,
            "error_message": run.error_message,
            "rate_limited_devices": sum(1 for row in devices if row.status == "rate_limited"),
            "failed_devices": sum(1 for row in devices if row.status == "failed"),
            "partial_devices": sum(1 for row in devices if row.status == "partial"),
        }

    async def backfill_historical(self, request: BackfillRequest, *, source: str = "backfill") -> dict[str, Any]:
        device_ids = self._resolve_backfill_device_ids(request)
        result = await self._backfill_device_ids(
            device_ids=device_ids,
            start_at=request.start_at,
            end_at=request.end_at,
            source=source,
        )
        if request.publish_snapshot and request.company_slug:
            cut_at = self._resolve_safe_publish_cut_for_range(
                company_slug=request.company_slug,
                range_end_at=request.end_at,
            )
            payload = self.dashboard.materialize_snapshot(
                request.company_slug,
                cut_at=cut_at,
                cut_status="succeeded",
            )
            result["published_cut_at"] = payload.get("meta", {}).get("publishedCutAt")
            result["recent_events"] = len(payload.get("recentEvents") or [])
            result["week_total"] = payload.get("dms", {}).get("semana", {}).get("total")
            result["last_dms_event_at"] = payload.get("meta", {}).get("lastDmsEventAt")
        return result

    async def _backfill_device_ids(
        self,
        *,
        device_ids: list[str],
        start_at: datetime,
        end_at: datetime,
        source: str,
        company_slug: str | None = None,
        rebuild_job_id: int | None = None,
        yield_to_live_harvest: bool = False,
        defer_on_rate_limit: bool = False,
        progress_callback: Callable[[int, int], Awaitable[None] | None] | None = None,
    ) -> dict[str, Any]:
        inserted = 0
        anomalies = 0
        latest_observed_at = None
        failed_devices: list[str] = []
        batch_total = AlarmBatchResult()
        batch_enabled = self._historical_batch_enabled(source=source, rebuild_job_id=rebuild_job_id)
        batch_company = None
        if company_slug:
            with suppress(KeyError):
                batch_company = self.registry.get(company_slug)
        per_device_pause = max(float(self.settings.catchup_batch_pause_seconds), 0.0) if source == "catchup" else 0.0
        for index, device_id in enumerate(device_ids):
            if yield_to_live_harvest:
                await self._yield_to_ready_harvests()
            try:
                rows = await self._fetch_historical_backfill_rows(
                    device_id=device_id,
                    start_at=start_at,
                    end_at=end_at,
                    source=source,
                    defer_on_rate_limit=defer_on_rate_limit,
                )
            except HistoricalBackfillDeferred:
                raise
            except Exception as exc:
                if source == "catchup":
                    raise
                failed_devices.append(device_id)
                anomalies += 1
                await self._record_anomaly(
                    source_type="backfill",
                    device_id=device_id,
                    company_slug=None,
                    received_at=utc_now(),
                    raw_event_time=None,
                    reason="backfill_device_failed",
                    payload={
                        "source": source,
                        "device_id": device_id,
                        "range_start": ensure_utc(start_at).isoformat(),
                        "range_end": ensure_utc(end_at).isoformat(),
                        "error": str(exc),
                    },
                )
                if progress_callback is not None:
                    maybe = progress_callback(index + 1, len(device_ids))
                    if maybe is not None:
                        await maybe
                if per_device_pause and index < len(device_ids) - 1:
                    await asyncio.sleep(per_device_pause)
                continue
            normalized_alarms: list[NormalizedAlarm] = []
            for row in rows:
                if not isinstance(row, dict):
                    anomalies += 1
                    await self._record_normalization_failure(
                        source_type="backfill_alarm",
                        payload={"device_id": device_id, "raw_row": row},
                        received_at=utc_now(),
                    )
                    continue
                alarm = self.howen.normalize_alarm(row)
                received_at = utc_now()
                if not alarm:
                    if self.howen.is_ignorable_historical_alarm(row):
                        continue
                    anomalies += 1
                    await self._record_normalization_failure(
                        source_type="backfill_alarm",
                        payload=row,
                        received_at=received_at,
                    )
                    continue
                normalized_alarms.append(alarm)
                if not batch_enabled:
                    ingest_result = await self.ingest_alarm(alarm, received_at=received_at, source=source)
                    if alarm.classification_status == "classified_dms" and alarm.occurred_at:
                        latest_observed_at = _max_datetime(latest_observed_at, alarm.occurred_at)
                    if ingest_result["inserted_alarm_event"]:
                        inserted += 1
            if batch_enabled and normalized_alarms:
                device_batch = await self.ingest_alarm_batch(
                    normalized_alarms,
                    source=source,
                    company=batch_company,
                    device_context={"device_id": device_id, "company_slug": company_slug},
                    batch_size=max(int(getattr(self.settings, "historical_batch_size", 500) or 500), 1),
                    rebuild_job_id=rebuild_job_id,
                )
                batch_total.merge(device_batch)
                inserted += device_batch.dms_inserted
                anomalies += device_batch.anomalies
                latest_observed_at = _max_datetime(latest_observed_at, device_batch.latest_observed_at)
            if progress_callback is not None:
                maybe = progress_callback(index + 1, len(device_ids))
                if maybe is not None:
                    await maybe
            if per_device_pause and index < len(device_ids) - 1:
                await asyncio.sleep(per_device_pause)
        return {
            "inserted": inserted,
            "anomalies": anomalies,
            "devices": len(device_ids),
            "failed_devices": failed_devices,
            "failed_count": len(failed_devices),
            "latest_observed_at": ensure_utc(latest_observed_at).isoformat() if latest_observed_at else None,
            "batch": batch_total.as_dict() if batch_enabled else None,
        }

    async def _fetch_historical_backfill_rows(
        self,
        *,
        device_id: str,
        start_at: datetime,
        end_at: datetime,
        source: str,
        defer_on_rate_limit: bool = False,
    ) -> list[dict[str, Any]]:
        local_start_at, local_end_at = self._historical_window_for_device(
            device_id=device_id,
            start_at=start_at,
            end_at=end_at,
        )
        max_retries = max(int(self.settings.backfill_rate_limit_max_retries), 0)
        base_cooldown = max(float(self.settings.backfill_rate_limit_cooldown_seconds), 1.0)
        max_cooldown = max(float(self.settings.backfill_rate_limit_max_cooldown_seconds), base_cooldown)

        attempt = 0
        while True:
            try:
                return await self.howen.fetch_historical_alarms_authorized(
                    device_id=device_id,
                    start_at=local_start_at,
                    end_at=local_end_at,
                    force_login=False,
                )
            except Exception as exc:
                if not self.howen.is_rate_limited(exc) or attempt >= max_retries:
                    raise
                cooldown = min(base_cooldown * (2**attempt), max_cooldown)
                next_retry_at = utc_now() + timedelta(seconds=cooldown)
                await self._record_anomaly(
                    source_type="backfill",
                    device_id=device_id,
                    company_slug=None,
                    received_at=utc_now(),
                    raw_event_time=None,
                    reason="backfill_rate_limited_retry",
                    payload={
                        "source": source,
                        "device_id": device_id,
                        "range_start": ensure_utc(start_at).isoformat(),
                        "range_end": ensure_utc(end_at).isoformat(),
                        "provider_range_start": local_start_at.isoformat(),
                        "provider_range_end": local_end_at.isoformat(),
                        "attempt": attempt + 1,
                        "max_retries": max_retries,
                        "cooldown_seconds": cooldown,
                        "next_retry_at": ensure_utc(next_retry_at).isoformat(),
                        "error": str(exc),
                    },
                )
                if defer_on_rate_limit:
                    raise HistoricalBackfillDeferred(
                        next_retry_at=next_retry_at,
                        message=(
                            "Proveedor limitando la reconstruccion historica. "
                            f"Reintento programado para {ensure_utc(next_retry_at).isoformat()}."
                        ),
                    ) from exc
                await asyncio.sleep(cooldown)
                attempt += 1

    def _historical_window_for_device(
        self,
        *,
        device_id: str,
        start_at: datetime,
        end_at: datetime,
    ) -> tuple[datetime, datetime]:
        start_utc = ensure_utc(start_at) or utc_now()
        end_utc = ensure_utc(end_at) or utc_now()
        with self.session_factory() as session:
            record = session.get(DeviceRecord, device_id)
        timezone_name = self.registry.timezone_for(
            device_id=device_id,
            fleet_id=record.fleet_id if record else None,
            slug=record.company_slug if record else None,
            fallback=self.settings.default_timezone,
        )
        return (
            as_timezone(start_utc, timezone_name) or start_utc,
            as_timezone(end_utc, timezone_name) or end_utc,
        )

    def _effective_catchup_batch_size(self, *, rate_limit_streak: int) -> int:
        configured = max(int(self.settings.catchup_device_batch_size), 1)
        if rate_limit_streak <= 0:
            return configured
        return 1

    def _effective_catchup_window(self, *, max_window: timedelta, rate_limit_streak: int) -> timedelta:
        if rate_limit_streak <= 0:
            return max_window
        min_window = timedelta(minutes=max(self.settings.catchup_overlap_minutes, 10))
        shrink_factor = 2 ** min(rate_limit_streak, 3)
        reduced_window = timedelta(seconds=max_window.total_seconds() / shrink_factor)
        return reduced_window if reduced_window > min_window else min_window

    def _plan_operational_catchup(
        self,
        *,
        force: bool,
        now_utc: datetime,
        last_successful_cursor: datetime | None,
        pending_range_start_at: datetime | None,
        pending_range_end_at: datetime | None,
        next_device_offset: int,
        next_retry_at: datetime | None,
        overlap: timedelta,
        stale_after: timedelta,
        bootstrap_span: timedelta,
        effective_window: timedelta,
    ) -> CatchupPlan | None:
        if next_retry_at and now_utc < next_retry_at:
            return None

        if pending_range_start_at and pending_range_end_at:
            start_at = pending_range_start_at
            end_at = min(pending_range_end_at, start_at + effective_window)
            return CatchupPlan(start_at=start_at, end_at=end_at, offset=max(next_device_offset, 0))

        if last_successful_cursor is None:
            start_at = now_utc - bootstrap_span
            end_at = min(now_utc, start_at + effective_window)
        elif force or now_utc - last_successful_cursor >= stale_after:
            start_at = last_successful_cursor - overlap
            end_at = min(now_utc, start_at + effective_window)
        else:
            # Keep a rolling overlap over the recent window so live gaps are recovered
            # without waiting for the cursor to become "stale".
            start_at = max(last_successful_cursor - overlap, now_utc - effective_window)
            end_at = now_utc

        if start_at >= end_at:
            return None
        return CatchupPlan(start_at=start_at, end_at=end_at, offset=0)

    async def _run_operational_catchup(self, *, force: bool = False) -> None:
        if self._maintenance_active():
            return
        overlap = timedelta(minutes=self.settings.catchup_overlap_minutes)
        stale_after = timedelta(minutes=self.settings.catchup_stale_after_minutes)
        bootstrap_span = timedelta(hours=self.settings.catchup_bootstrap_hours)
        max_window = timedelta(minutes=self.settings.catchup_max_window_minutes)
        run_budget_seconds = max(float(self.settings.catchup_run_time_budget_seconds), 1.0)
        batch_pause_seconds = max(float(self.settings.catchup_batch_pause_seconds), 0.0)
        deadline = monotonic() + run_budget_seconds

        for company in self.registry.all():
            if not self.registry.is_operational(company):
                continue
            lock = self._catchup_locks.setdefault(company.slug, asyncio.Lock())
            if lock.locked():
                continue
            async with lock:
                device_ids = self._list_company_device_ids(company.slug)
                if not device_ids:
                    continue
                while monotonic() < deadline:
                    now_utc = utc_now()
                    with self.session_factory() as session:
                        cursor = session.get(CatchupCursor, company.slug)
                        last_successful_cursor = None
                        if cursor:
                            last_successful_cursor = (
                                ensure_utc(cursor.last_successful_catchup_cursor_at)
                                or ensure_utc(cursor.last_successful_catchup_observed_at)
                            )
                            pending_range_start_at = ensure_utc(cursor.pending_range_start_at)
                            pending_range_end_at = ensure_utc(cursor.pending_range_end_at)
                            next_device_offset = max(cursor.next_device_offset or 0, 0)
                            next_retry_at = ensure_utc(cursor.next_retry_at)
                            rate_limit_streak = max(cursor.rate_limit_streak or 0, 0)
                        else:
                            pending_range_start_at = None
                            pending_range_end_at = None
                            next_device_offset = 0
                            next_retry_at = None
                            rate_limit_streak = 0

                    effective_window = self._effective_catchup_window(
                        max_window=max_window,
                        rate_limit_streak=rate_limit_streak,
                    )
                    effective_batch_size = self._effective_catchup_batch_size(rate_limit_streak=rate_limit_streak)

                    plan = self._plan_operational_catchup(
                        force=force,
                        now_utc=now_utc,
                        last_successful_cursor=last_successful_cursor,
                        pending_range_start_at=pending_range_start_at,
                        pending_range_end_at=pending_range_end_at,
                        next_device_offset=next_device_offset,
                        next_retry_at=next_retry_at,
                        overlap=overlap,
                        stale_after=stale_after,
                        bootstrap_span=bootstrap_span,
                        effective_window=effective_window,
                    )
                    if plan is None:
                        break
                    start_at = plan.start_at
                    end_at = plan.end_at
                    offset = min(plan.offset, len(device_ids))

                    batch_device_ids = device_ids[offset : offset + effective_batch_size]
                    if not batch_device_ids:
                        with self.session_factory() as session:
                            cursor = session.get(CatchupCursor, company.slug) or CatchupCursor(company_slug=company.slug)
                            cursor.last_successful_catchup_cursor_at = end_at
                            cursor.pending_range_start_at = None
                            cursor.pending_range_end_at = None
                            cursor.next_device_offset = 0
                            cursor.last_error = None
                            session.add(cursor)
                            session.commit()
                        continue

                    try:
                        result = await self._backfill_device_ids(
                            device_ids=batch_device_ids,
                            start_at=start_at,
                            end_at=end_at,
                            source="catchup",
                        )
                    except Exception as exc:
                        is_rate_limited = self.howen.is_rate_limited(exc)
                        retry_reason = f"catchup_failed:{type(exc).__name__}"
                        await self._record_anomaly(
                            source_type="catchup",
                            device_id=None,
                            company_slug=company.slug,
                            received_at=utc_now(),
                            raw_event_time=None,
                            reason="catchup_rate_limited" if is_rate_limited else retry_reason,
                            payload={
                                "company_slug": company.slug,
                                "error": str(exc),
                                "range_start": start_at.isoformat(),
                                "range_end": end_at.isoformat(),
                                "offset": offset,
                                "batch_size": len(batch_device_ids),
                            },
                        )
                        with self.session_factory() as session:
                            cursor = session.get(CatchupCursor, company.slug) or CatchupCursor(company_slug=company.slug)
                            next_streak = (cursor.rate_limit_streak or 0) + 1 if is_rate_limited else 0
                            if is_rate_limited:
                                cooldown_seconds = min(
                                    self.settings.catchup_rate_limit_base_seconds * (2 ** max(next_streak - 1, 0)),
                                    self.settings.catchup_rate_limit_max_seconds,
                                )
                                retry_at = now_utc + timedelta(seconds=cooldown_seconds)
                                cursor.pending_range_start_at = start_at
                                cursor.pending_range_end_at = end_at
                                cursor.next_device_offset = offset
                            else:
                                retry_at = now_utc + timedelta(seconds=self.settings.catchup_error_retry_seconds)
                            cursor.last_attempt_at = now_utc
                            cursor.last_error = str(exc)
                            cursor.next_retry_at = retry_at
                            cursor.rate_limit_streak = next_streak
                            session.add(cursor)
                            session.commit()
                        break

                    latest_observed_at = parse_timestamp(result.get("latest_observed_at"))
                    next_offset = offset + len(batch_device_ids)
                    range_completed = next_offset >= len(device_ids)
                    with self.session_factory() as session:
                        cursor = session.get(CatchupCursor, company.slug) or CatchupCursor(company_slug=company.slug)
                        cursor.last_attempt_at = now_utc
                        cursor.last_successful_catchup_observed_at = latest_observed_at or cursor.last_successful_catchup_observed_at
                        if range_completed:
                            cursor.last_successful_catchup_cursor_at = end_at
                            cursor.pending_range_start_at = None
                            cursor.pending_range_end_at = None
                            cursor.next_device_offset = 0
                        else:
                            cursor.pending_range_start_at = start_at
                            cursor.pending_range_end_at = end_at
                            cursor.next_device_offset = next_offset
                        cursor.next_retry_at = None
                        cursor.rate_limit_streak = 0
                        cursor.last_error = None
                        session.add(cursor)
                        session.commit()
                    if result["inserted"] or result["anomalies"]:
                        self.mark_dirty()

                    if not range_completed:
                        if batch_pause_seconds:
                            await asyncio.sleep(batch_pause_seconds)
                        continue

                    if not force and latest_observed_at and now_utc - latest_observed_at < stale_after:
                        break

    async def replay_status_anomalies(self, *, limit: int = 2000) -> dict[str, int]:
        with self.session_factory() as session:
            anomalies = list(
                session.scalars(
                    select(IngestionAnomaly)
                    .where(
                        IngestionAnomaly.source_type == "status",
                        IngestionAnomaly.reason == "normalization_failed",
                    )
                    .order_by(IngestionAnomaly.received_at.desc())
                    .limit(limit)
                )
            )

        processed = 0
        inserted = 0
        skipped = 0
        for anomaly in reversed(anomalies):
            try:
                payload = json.loads(anomaly.payload_json)
            except json.JSONDecodeError:
                skipped += 1
                continue
            status = self.howen.normalize_status(payload)
            if not status:
                skipped += 1
                continue
            received_at = ensure_utc(anomaly.received_at) or utc_now()
            valid = await self._validate_temporal_integrity(
                source_type="status_replay",
                device_id=status.device_id,
                fleet_id=status.fleet_id,
                observed_at=status.observed_at,
                raw_event_time=status.raw_event_time,
                received_at=received_at,
                payload=status.raw,
            )
            if not valid:
                skipped += 1
                continue
            await self.ingest_status(status, received_at=received_at, update_feed_state=False)
            processed += 1
            inserted += 1
        return {
            "processed": processed,
            "inserted": inserted,
            "skipped": skipped,
            "loaded": len(anomalies),
        }

    async def sync_devices(self, *, force: bool) -> None:
        if not force and not self._should_sync_devices():
            return
        rows = await self.howen.fetch_devices_authorized(force_login=False)
        now = utc_now()
        with self.session_factory() as session:
            for row in rows:
                device_id = str(row.get("deviceno") or row.get("deviceID") or row.get("deviceid") or "").strip()
                if not device_id:
                    continue
                fleet_id = row.get("fleetid") or row.get("fleetId")
                company = self.registry.resolve_company(device_id=device_id, fleet_id=fleet_id)
                raw_plate = self.howen.extract_plate_candidate(row)
                normalized_plate = self.registry.normalize_plate(company, raw_plate) if company else self.registry.normalize_plate_any(raw_plate)
                record = session.get(DeviceRecord, device_id) or DeviceRecord(device_id=device_id)
                previous_identity = (record.company_slug, record.fleet_id, record.plate_no)
                record.plate_no = normalized_plate or record.plate_no
                record.company_slug = company.slug if company else record.company_slug
                record.fleet_id = fleet_id or record.fleet_id
                record.fleet_name = row.get("fleetname") or row.get("fleetName") or record.fleet_name
                record.device_name = row.get("devicename") or row.get("deviceName") or record.device_name
                record.record_source = "live"
                record.last_seen_at = record.last_seen_at or now
                session.add(record)
                if previous_identity != (record.company_slug, record.fleet_id, record.plate_no):
                    self._propagate_company_assignment(
                        session,
                        device_id=record.device_id,
                        company_slug=record.company_slug,
                        plate_no=record.plate_no,
                        fleet_id=record.fleet_id,
                    )
            state = session.get(IngestState, "global")
            if state:
                state.last_device_sync_at = now
            session.commit()
        self._last_device_sync_at = now
        self.mark_dirty()

    async def ingest_status(self, status: NormalizedStatus, *, received_at, update_feed_state: bool = True) -> None:
        received_at = ensure_utc(received_at) or utc_now()
        observed_at = ensure_utc(status.observed_at) or status.observed_at
        anomaly_reasons: list[str] = []
        with self.session_factory() as session:
            record = session.get(DeviceRecord, status.device_id) or DeviceRecord(device_id=status.device_id)
            previous_identity = (record.company_slug, record.fleet_id, record.plate_no)
            effective_fleet_id = status.fleet_id or record.fleet_id
            company = self.registry.resolve_company(device_id=status.device_id, fleet_id=effective_fleet_id)
            if company is None and record.company_slug:
                with suppress(KeyError):
                    company = self.registry.get(record.company_slug)
            company_slug = company.slug if company else record.company_slug
            normalized_plate = self.registry.normalize_plate(company, status.plate_no) if company else self.registry.normalize_plate_any(status.plate_no)
            timezone_name = self.registry.timezone_for(
                device_id=status.device_id,
                fleet_id=effective_fleet_id,
                slug=company_slug,
                fallback=self.settings.default_timezone,
            )
            snapshot_date = to_local_date(observed_at, timezone_name)
            previous_total_km = record.last_total_km
            validated_total_km = status.total_km
            validated_day_km = status.day_km
            validation_status = "valid"
            validation_reason: str | None = None

            if (
                validated_total_km is not None
                and previous_total_km is not None
                and validated_total_km + 0.1 < previous_total_km
            ):
                validation_status = "invalid"
                validation_reason = "total_regression"
                anomaly_reasons.append(validation_reason)
                validated_total_km = None

            total_reference = validated_total_km if validated_total_km is not None else previous_total_km
            if (
                validated_day_km is not None
                and total_reference is not None
                and validated_day_km > total_reference + 0.1
            ):
                validation_status = "invalid"
                validation_reason = _append_reason(validation_reason, "day_gt_total")
                anomaly_reasons.append("day_gt_total")
                validated_day_km = None

            record.plate_no = normalized_plate or record.plate_no
            record.company_slug = company_slug or record.company_slug
            record.fleet_id = effective_fleet_id or record.fleet_id
            record.driver_name = status.driver_name or record.driver_name
            record.device_name = status.device_name or record.device_name
            record.last_seen_at = observed_at
            record.last_received_at = received_at
            if validated_total_km is not None:
                record.last_total_km = validated_total_km
            if validated_day_km is not None:
                record.last_day_km = validated_day_km
            record.record_source = "live"
            record.raw_total_value = status.raw_total_value
            record.raw_day_value = status.raw_day_value
            record.km_validation_status = validation_status
            record.km_validation_reason = validation_reason
            record.raw_payload = json.dumps(status.raw, ensure_ascii=True)
            session.add(record)
            if validated_total_km is not None:
                session.add(
                    MileageReading(
                        device_id=status.device_id,
                        plate_no=record.plate_no,
                        fleet_id=record.fleet_id,
                        recorded_at=observed_at,
                        total_km=validated_total_km,
                        day_km=validated_day_km,
                        source="status",
                    )
                )
            if company_slug and (validated_total_km is not None or validated_day_km is not None):
                observation_key = hashlib.sha256(
                    (
                        f"status_80003|{company_slug}|{status.device_id}|"
                        f"{observed_at.isoformat()}"
                    ).encode("utf-8")
                ).hexdigest()
                observation = session.scalar(
                    select(MileageObservation).where(
                        MileageObservation.observation_key == observation_key
                    )
                )
                if not observation:
                    observation = MileageObservation(
                        observation_key=observation_key,
                        company_slug=company_slug,
                        device_id=status.device_id,
                        observed_at=observed_at,
                        received_at=received_at,
                        source="status_80003",
                    )
                observation.fleet_id = record.fleet_id
                observation.plate_no = record.plate_no
                observation.total_km = validated_total_km
                observation.day_km = validated_day_km
                observation.raw_total_value = status.raw_total_value
                observation.raw_day_value = status.raw_day_value
                observation.validation_status = validation_status
                observation.validation_reason = validation_reason
                observation.payload_json = json.dumps(status.raw, ensure_ascii=True)
                session.add(observation)
            if previous_identity != (record.company_slug, record.fleet_id, record.plate_no):
                self._propagate_company_assignment(
                    session,
                    device_id=status.device_id,
                    company_slug=record.company_slug,
                    plate_no=record.plate_no,
                    fleet_id=record.fleet_id,
                )

            total_for_snapshot = (
                validated_total_km
                if validated_total_km is not None
                else record.last_total_km
            )
            today_local = received_at.astimezone(ZoneInfo(timezone_name)).date()
            snapshot = session.scalar(
                select(DailyMileageSnapshot).where(
                    DailyMileageSnapshot.device_id == status.device_id,
                    DailyMileageSnapshot.snapshot_date == snapshot_date,
                )
            )
            may_update_snapshot = not (
                snapshot
                and snapshot.snapshot_date < today_local
                and snapshot.closure_status in {"closed", "repaired"}
            )
            if total_for_snapshot is not None and may_update_snapshot:
                if not snapshot:
                    snapshot = DailyMileageSnapshot(
                        device_id=status.device_id,
                        snapshot_date=snapshot_date,
                        observed_at=observed_at,
                        total_km=total_for_snapshot,
                        day_km=validated_day_km,
                        plate_no=record.plate_no,
                        company_slug=record.company_slug,
                        fleet_id=record.fleet_id,
                        raw_total_value=status.raw_total_value,
                        raw_day_value=status.raw_day_value,
                        km_validation_status=validation_status,
                        km_validation_reason=validation_reason,
                        source="live",
                        closure_status="provisional",
                        coverage_status="covered" if validated_day_km is not None else "missing_day_km",
                        sample_count=1,
                        first_observed_at=observed_at,
                        last_observed_at=observed_at,
                    )
                else:
                    snapshot.observed_at = observed_at
                    snapshot.plate_no = record.plate_no
                    snapshot.company_slug = record.company_slug
                    snapshot.fleet_id = record.fleet_id
                    snapshot.raw_total_value = status.raw_total_value
                    snapshot.raw_day_value = status.raw_day_value
                    snapshot.km_validation_status = validation_status
                    snapshot.km_validation_reason = validation_reason
                    snapshot.source = "live"
                    snapshot.closure_status = "provisional"
                    snapshot.coverage_status = "covered" if validated_day_km is not None else "missing_day_km"
                    snapshot.sample_count = max(snapshot.sample_count or 0, 0) + 1
                    snapshot.first_observed_at = snapshot.first_observed_at or observed_at
                    snapshot.last_observed_at = observed_at
                    if validated_total_km is not None:
                        snapshot.total_km = validated_total_km
                    if validated_day_km is not None:
                        snapshot.day_km = validated_day_km
                session.add(snapshot)

            state = session.get(IngestState, "global")
            if state and update_feed_state:
                state.mode = "live"
                state.connection_state = "connected"
                state.last_message_at = _max_datetime(state.last_message_at, received_at)
                state.last_cycle_received_at = _max_datetime(state.last_cycle_received_at, received_at)
                state.last_event_observed_at = _max_datetime(state.last_event_observed_at, observed_at)
                state.last_status_at = _max_datetime(state.last_status_at, observed_at)
                state.last_error = None
            session.commit()
        for reason in anomaly_reasons:
            await self._record_anomaly(
                source_type="status",
                device_id=status.device_id,
                company_slug=company_slug,
                received_at=received_at,
                raw_event_time=status.raw_event_time,
                reason=reason,
                payload=status.raw,
            )
        self.mark_dirty()

    def _historical_batch_enabled(self, *, source: str, rebuild_job_id: int | None) -> bool:
        mode = str(getattr(self.settings, "historical_batch_mode", "activation_only") or "activation_only")
        if mode == "off":
            return False
        if mode == "activation_only":
            return rebuild_job_id is not None
        return source in {"harvest", "backfill"}

    def _resolve_batch_device_context(
        self,
        *,
        alarm: NormalizedAlarm,
        company: Any | None,
        device_context: dict[str, Any] | None,
    ) -> dict[str, Any]:
        context = dict(device_context or {})
        with self.session_factory() as session:
            record = session.get(DeviceRecord, alarm.device_id)
            fleet_id = context.get("fleet_id") or alarm.fleet_id or (record.fleet_id if record else None)
            resolved_company = company or self.registry.resolve_company(
                device_id=alarm.device_id,
                fleet_id=fleet_id,
            )
            if resolved_company is None and record and record.company_slug:
                with suppress(KeyError):
                    resolved_company = self.registry.get(record.company_slug)
            company_slug = (
                getattr(resolved_company, "slug", None)
                or context.get("company_slug")
                or (record.company_slug if record else None)
            )
            plate_no = self.registry.canonical_plate(
                alarm.device_id,
                context.get("plate_no"),
                record.plate_no if record else None,
                alarm.plate_no,
            )
            if resolved_company is not None:
                plate_no = self.registry.normalize_plate(resolved_company, plate_no)
            timezone_name = self.registry.timezone_for(
                device_id=alarm.device_id,
                fleet_id=fleet_id,
                slug=company_slug,
                fallback=self.settings.default_timezone,
            )
            return {
                "device_id": alarm.device_id,
                "company": resolved_company,
                "company_slug": company_slug,
                "fleet_id": fleet_id,
                "plate_no": plate_no,
                "driver_name": context.get("driver_name") or alarm.driver_name or (record.driver_name if record else None),
                "timezone_name": timezone_name,
            }

    def _prepare_alarm_batch_rows(
        self,
        alarms: list[NormalizedAlarm],
        *,
        source: str,
        company: Any | None,
        device_context: dict[str, Any] | None,
        received_at: datetime,
    ) -> tuple[list[PreparedAlarmRow], AlarmBatchResult, dict[str, Any]]:
        result = AlarmBatchResult(provider_rows=len(alarms))
        if not alarms:
            return [], result, dict(device_context or {})
        device_ids = {alarm.device_id for alarm in alarms}
        if len(device_ids) != 1:
            raise ValueError("ingest_alarm_batch expects alarms for exactly one device")
        context = self._resolve_batch_device_context(
            alarm=alarms[0],
            company=company,
            device_context=device_context,
        )
        tolerance = timedelta(minutes=self.settings.anomaly_future_tolerance_minutes)
        seen_provider_keys: set[str] = set()
        seen_guids: set[str] = set()
        seen_fuzzy: set[tuple[Any, ...]] = set()
        prepared: list[PreparedAlarmRow] = []

        for alarm in alarms:
            occurred_at = ensure_utc(alarm.occurred_at) or alarm.occurred_at
            occurred_at, temporal_resolution = self._resolve_alarm_occurred_at(
                alarm=alarm,
                occurred_at=occurred_at,
                received_at=received_at,
                timezone_name=context["timezone_name"],
            )
            start_at = ensure_utc(alarm.start_at)
            end_at = ensure_utc(alarm.end_at)
            company_slug = context.get("company_slug")
            fleet_id = alarm.fleet_id or context.get("fleet_id")
            plate_no = self.registry.canonical_plate(
                alarm.device_id,
                context.get("plate_no"),
                alarm.plate_no,
            )
            if context.get("company") is not None:
                plate_no = self.registry.normalize_plate(context["company"], plate_no)
            provider_event_key = _build_provider_event_key(
                company_slug=company_slug,
                device_id=alarm.device_id,
                category=alarm.category,
                raw_alarm_type=alarm.raw_alarm_type,
                raw_tp=alarm.raw_tp,
                raw_event_code=alarm.raw_event_code,
                occurred_at=occurred_at,
                start_at=start_at,
                end_at=end_at,
            )
            temporal_valid = occurred_at is not None and occurred_at - received_at <= tolerance
            temporal_status = "accepted" if temporal_valid else "future_rejected"
            if not temporal_valid:
                ingest_result = "future_rejected"
            elif alarm.classification_status == "classified_dms":
                ingest_result = "pending_alarm_upsert"
            elif alarm.classification_status == "classified_non_dms":
                ingest_result = "kept_raw_only_non_dms"
                result.non_dms += 1
            elif alarm.classification_status == "unmapped":
                ingest_result = "kept_raw_only_unmapped"
                result.unmapped += 1
            else:
                ingest_result = "kept_raw_only"

            row = PreparedAlarmRow(
                alarm=alarm,
                provider_event_key=provider_event_key,
                company_slug=company_slug,
                fleet_id=fleet_id,
                plate_no=plate_no or context.get("plate_no"),
                driver_name=alarm.driver_name or context.get("driver_name"),
                occurred_at=ensure_utc(occurred_at),
                received_at=received_at,
                start_at=start_at,
                end_at=end_at,
                temporal_status=temporal_status,
                temporal_resolution=temporal_resolution,
                ingest_result=ingest_result,
                payload_json=json.dumps(alarm.raw, ensure_ascii=True),
            )
            if (
                (provider_event_key and provider_event_key in seen_provider_keys)
                or alarm.guid in seen_guids
                or row.fuzzy_key in seen_fuzzy
            ):
                result.duplicates += 1
                continue
            if provider_event_key:
                seen_provider_keys.add(provider_event_key)
            seen_guids.add(alarm.guid)
            seen_fuzzy.add(row.fuzzy_key)
            if temporal_valid and alarm.classification_status == "classified_dms":
                result.latest_observed_at = _max_datetime(result.latest_observed_at, occurred_at)
            if not temporal_valid:
                result.temporal_rejected += 1
            prepared.append(row)
        result.prepared_rows = len(prepared)
        return prepared, result, context

    async def ingest_alarm_batch(
        self,
        alarms: list[NormalizedAlarm],
        *,
        source: str,
        company: Any | None = None,
        device_context: dict[str, Any] | None = None,
        batch_size: int = 500,
        rebuild_job_id: int | None = None,
    ) -> AlarmBatchResult:
        received_at = utc_now()
        prepared, result, context = self._prepare_alarm_batch_rows(
            alarms,
            source=source,
            company=company,
            device_context=device_context,
            received_at=received_at,
        )
        if not prepared:
            return result

        batch_size = max(int(batch_size or 500), 1)
        final_device_context = dict(context)
        final_device_context["batch_max_observed"] = max(
            (row.occurred_at for row in prepared if row.occurred_at),
            default=None,
        )
        final_device_context["batch_max_received"] = max(row.received_at for row in prepared)
        final_device_context["batch_driver_name"] = next(
            (row.driver_name for row in reversed(prepared) if row.driver_name),
            context.get("driver_name"),
        )
        try:
            for offset in range(0, len(prepared), batch_size):
                chunk = prepared[offset : offset + batch_size]
                is_last_chunk = offset + len(chunk) >= len(prepared)
                chunk_result = await asyncio.to_thread(
                    self._ingest_alarm_batch_chunk,
                    chunk,
                    source,
                    rebuild_job_id,
                    len(prepared),
                    is_last_chunk,
                    final_device_context if is_last_chunk else None,
                )
                result.merge(chunk_result)
        except Exception:
            result.errors += 1
            raise
        self.mark_dirty()
        return result

    def _ingest_alarm_batch_chunk(
        self,
        rows: list[PreparedAlarmRow],
        source: str,
        rebuild_job_id: int | None,
        device_rows_total: int,
        is_last_chunk: bool,
        device_context: dict[str, Any] | None,
    ) -> AlarmBatchResult:
        result = AlarmBatchResult()
        if not rows:
            return result
        device_id = rows[0].alarm.device_id
        provider_keys = [row.provider_event_key for row in rows if row.provider_event_key]
        guids = [row.alarm.guid for row in rows]
        observed = [row.occurred_at for row in rows if row.occurred_at]
        range_start = min(observed) if observed else None
        range_end = max(observed) if observed else None

        with self.session_factory() as session:
            preload_conditions: list[Any] = []
            if provider_keys:
                preload_conditions.append(HowenAlarmRaw.provider_event_key.in_(provider_keys))
            if guids:
                preload_conditions.append(HowenAlarmRaw.guid.in_(guids))
            if range_start and range_end:
                preload_conditions.append(
                    and_(
                        HowenAlarmRaw.device_id == device_id,
                        HowenAlarmRaw.occurred_at >= range_start,
                        HowenAlarmRaw.occurred_at <= range_end,
                    )
                )
            existing_raw = list(
                session.scalars(select(HowenAlarmRaw).where(or_(*preload_conditions)))
            ) if preload_conditions else []

            event_conditions: list[Any] = []
            if provider_keys:
                event_conditions.append(AlarmEvent.provider_event_key.in_(provider_keys))
            if guids:
                event_conditions.append(AlarmEvent.guid.in_(guids))
            if range_start and range_end:
                event_conditions.append(
                    and_(
                        AlarmEvent.device_id == device_id,
                        AlarmEvent.occurred_at >= range_start,
                        AlarmEvent.occurred_at <= range_end,
                    )
                )
            existing_events = list(
                session.scalars(select(AlarmEvent).where(or_(*event_conditions)))
            ) if event_conditions else []

            raw_by_key = {item.provider_event_key: item for item in existing_raw if item.provider_event_key}
            raw_by_guid = {item.guid: item for item in existing_raw}
            raw_by_fuzzy = {_raw_fuzzy_key(item): item for item in existing_raw}
            event_by_key = {item.provider_event_key: item for item in existing_events if item.provider_event_key}
            event_by_guid = {item.guid: item for item in existing_events}
            event_by_fuzzy = {_event_fuzzy_key(item): item for item in existing_events}

            raw_mappings: list[dict[str, Any]] = []
            event_mappings: list[dict[str, Any]] = []
            audit_candidates: list[dict[str, Any]] = []
            anomaly_candidates: list[dict[str, Any]] = []
            now = utc_now()

            for row in rows:
                alarm = row.alarm
                existing_raw_row = (
                    raw_by_key.get(row.provider_event_key)
                    or raw_by_guid.get(alarm.guid)
                    or raw_by_fuzzy.get(row.fuzzy_key)
                )
                raw_guid = existing_raw_row.guid if existing_raw_row else alarm.guid
                if existing_raw_row:
                    result.raw_updated += 1
                else:
                    result.raw_inserted += 1

                existing_event = None
                ingest_result = row.ingest_result
                if row.temporal_valid and alarm.classification_status == "classified_dms":
                    existing_event = (
                        event_by_key.get(row.provider_event_key)
                        or event_by_guid.get(alarm.guid)
                        or event_by_fuzzy.get(row.fuzzy_key)
                    )
                    event_guid = existing_event.guid if existing_event else raw_guid
                    if existing_event:
                        result.dms_updated += 1
                        ingest_result = "updated_alarm_event"
                    else:
                        result.dms_inserted += 1
                        ingest_result = "inserted_alarm_event"
                    event_mapping = {
                        "guid": event_guid,
                        "provider_event_key": row.provider_event_key,
                        "device_id": alarm.device_id,
                        "plate_no": row.plate_no,
                        "company_slug": row.company_slug,
                        "fleet_id": row.fleet_id,
                        "driver_name": row.driver_name,
                        "category": alarm.category,
                        "subtype": alarm.subtype,
                        "mapping_source": alarm.mapping_source,
                        "classification_status": alarm.classification_status,
                        "visibility_status": alarm.visibility_status,
                        "event_code": alarm.event_code,
                        "raw_alarm_type": alarm.raw_alarm_type,
                        "raw_tp": alarm.raw_tp,
                        "raw_event_code": alarm.raw_event_code,
                        "occurred_at": row.occurred_at,
                        "received_at": row.received_at,
                        "start_at": row.start_at,
                        "end_at": row.end_at,
                        "raw_event_time": alarm.raw_event_time,
                        "latitude": alarm.latitude,
                        "longitude": alarm.longitude,
                        "total_mileage_km": alarm.total_mileage_km,
                        "source": source,
                        # The complete provider payload lives only in howen_alarm_raw.
                        "raw_payload": None,
                    }
                    event_mappings.append(event_mapping)
                elif not row.temporal_valid:
                    result.anomalies += 1
                    anomaly_candidates.append(
                        {
                            "source_type": f"{source}_alarm",
                            "device_id": alarm.device_id,
                            "company_slug": row.company_slug,
                            "received_at": row.received_at,
                            "raw_event_time": alarm.raw_event_time,
                            "reason": "future_timestamp",
                            "payload_json": row.payload_json,
                        }
                    )

                raw_mapping = {
                    "guid": raw_guid,
                    "provider_event_key": row.provider_event_key,
                    "company_slug": row.company_slug,
                    "device_id": alarm.device_id,
                    "fleet_id": row.fleet_id,
                    "plate_no": row.plate_no,
                    "source": source,
                    "occurred_at": row.occurred_at,
                    "received_at": row.received_at,
                    "raw_alarm_type": alarm.raw_alarm_type,
                    "raw_tp": alarm.raw_tp,
                    "raw_event_code": alarm.raw_event_code,
                    "raw_event_time": alarm.raw_event_time,
                    "classification_status": alarm.classification_status,
                    "mapped_category": alarm.category,
                    "mapping_source": alarm.mapping_source,
                    "temporal_status": row.temporal_status,
                    "ingest_result": ingest_result,
                    "payload_json": row.payload_json,
                    "updated_at": now,
                }
                raw_mappings.append(raw_mapping)
                audit_candidates.extend(
                    self._batch_audit_rows(
                        row=row,
                        source=source,
                        ingest_result=ingest_result,
                    )
                )

            _bulk_upsert_rows(session, HowenAlarmRaw, raw_mappings, conflict_columns=["guid"])
            _bulk_upsert_rows(session, AlarmEvent, event_mappings, conflict_columns=["guid"])
            self._insert_batch_audits(session, audit_candidates)
            self._insert_batch_anomalies(session, anomaly_candidates)

            if is_last_chunk and device_context is not None:
                self._update_device_after_alarm_batch(session, device_context, rows)

            if rebuild_job_id is not None:
                job = session.get(CompanyHistoricalRebuildJob, rebuild_job_id)
                if job:
                    if job.current_device_id != device_id:
                        job.rows_total = max(
                            job.rows_total or 0,
                            (job.rows_processed or 0) + device_rows_total,
                        )
                    job.rows_processed = (job.rows_processed or 0) + len(rows)
                    job.last_heartbeat_at = now
                    job.current_device_id = None if is_last_chunk else device_id
                    job.phase = "fetching" if is_last_chunk else "batch_upsert"
                    session.add(job)
            session.commit()
            result.chunks_committed = 1
        return result

    def _batch_audit_rows(
        self,
        *,
        row: PreparedAlarmRow,
        source: str,
        ingest_result: str,
    ) -> list[dict[str, Any]]:
        alarm = row.alarm
        base = {
            "guid": alarm.guid,
            "provider_event_key": row.provider_event_key,
            "company_slug": row.company_slug,
            "device_id": alarm.device_id,
            "fleet_id": row.fleet_id,
            "plate_no": row.plate_no,
            "observed_at": row.occurred_at,
            "received_at": row.received_at,
            "raw_alarm_type": alarm.raw_alarm_type,
            "raw_tp": alarm.raw_tp,
            "raw_event_code": alarm.raw_event_code,
            "payload_json": "{}",
        }
        output: list[dict[str, Any]] = []
        if alarm.classification_status == "unmapped" or (
            alarm.classification_status == "classified_dms"
            and (alarm.mapping_source or "unknown") == "unknown"
        ):
            output.append({**base, "stage": f"mapping_review_{source}", "reason": alarm.classification_status or "unknown"})
        if row.temporal_resolution:
            output.append(
                {**base, "stage": f"temporal_resolution_{source}", "reason": row.temporal_resolution}
            )
        if not row.temporal_valid:
            output.append({**base, "stage": f"{source}_alarm", "reason": "future_timestamp"})
        return output

    def _insert_batch_audits(self, session: Any, candidates: list[dict[str, Any]]) -> None:
        if not candidates:
            return
        guids = sorted({row["guid"] for row in candidates if row.get("guid")})
        stages = sorted({row["stage"] for row in candidates})
        reasons = sorted({row["reason"] for row in candidates})
        existing = set(
            session.execute(
                select(AlarmEventAudit.guid, AlarmEventAudit.stage, AlarmEventAudit.reason).where(
                    AlarmEventAudit.guid.in_(guids),
                    AlarmEventAudit.stage.in_(stages),
                    AlarmEventAudit.reason.in_(reasons),
                )
            ).all()
        ) if guids else set()
        unique_rows: list[dict[str, Any]] = []
        seen = set(existing)
        for row in candidates:
            key = (row.get("guid"), row["stage"], row["reason"])
            if row.get("guid") and key in seen:
                continue
            seen.add(key)
            unique_rows.append(row)
        if unique_rows:
            session.execute(insert(AlarmEventAudit), unique_rows)

    def _insert_batch_anomalies(self, session: Any, candidates: list[dict[str, Any]]) -> None:
        if not candidates:
            return
        device_ids = sorted({row["device_id"] for row in candidates if row.get("device_id")})
        existing = set(
            session.execute(
                select(
                    IngestionAnomaly.device_id,
                    IngestionAnomaly.source_type,
                    IngestionAnomaly.reason,
                    IngestionAnomaly.raw_event_time,
                ).where(
                    IngestionAnomaly.device_id.in_(device_ids),
                    IngestionAnomaly.reason == "future_timestamp",
                )
            ).all()
        ) if device_ids else set()
        unique_rows: list[dict[str, Any]] = []
        seen = set(existing)
        for row in candidates:
            key = (row.get("device_id"), row["source_type"], row["reason"], row.get("raw_event_time"))
            if key in seen:
                continue
            seen.add(key)
            unique_rows.append(row)
        if unique_rows:
            session.execute(insert(IngestionAnomaly), unique_rows)

    def _update_device_after_alarm_batch(
        self,
        session: Any,
        context: dict[str, Any],
        rows: list[PreparedAlarmRow],
    ) -> None:
        if not rows:
            return
        max_observed = context.get("batch_max_observed")
        max_received = context.get("batch_max_received")
        driver_name = context.get("batch_driver_name") or context.get("driver_name")
        record = session.get(DeviceRecord, context["device_id"])
        if not record:
            record = DeviceRecord(device_id=context["device_id"])
        record.plate_no = context.get("plate_no") or record.plate_no
        record.company_slug = context.get("company_slug") or record.company_slug
        record.fleet_id = context.get("fleet_id") or record.fleet_id
        record.driver_name = driver_name or record.driver_name
        record.last_seen_at = _max_datetime(record.last_seen_at, max_observed)
        record.last_received_at = _max_datetime(record.last_received_at, max_received)
        session.add(record)

    async def ingest_alarm(self, alarm: NormalizedAlarm, *, received_at, source: str) -> dict[str, Any]:
        received_at = ensure_utc(received_at) or utc_now()
        occurred_at = ensure_utc(alarm.occurred_at) or alarm.occurred_at
        raw_ingest_result = "stored_raw"
        inserted_alarm_event = False
        inserted_raw_row = False
        provider_event_key: str | None = None

        with self.session_factory() as session:
            record = session.get(DeviceRecord, alarm.device_id)
            effective_fleet_id = alarm.fleet_id or (record.fleet_id if record else None)
            company = self.registry.resolve_company(device_id=alarm.device_id, fleet_id=effective_fleet_id)
            plate_no = self.registry.canonical_plate(
                alarm.device_id,
                record.plate_no if record else None,
                alarm.plate_no,
            )
            if company is None and plate_no:
                matched_record = session.scalar(
                    select(DeviceRecord)
                    .where(DeviceRecord.plate_no == plate_no)
                    .order_by(DeviceRecord.last_received_at.desc().nullslast(), DeviceRecord.last_seen_at.desc().nullslast())
                )
                if matched_record:
                    if record is None or not record.company_slug:
                        record = matched_record
                    effective_fleet_id = effective_fleet_id or matched_record.fleet_id
                    if matched_record.company_slug:
                        with suppress(KeyError):
                            company = self.registry.get(matched_record.company_slug)
            if company is None and record and record.company_slug:
                with suppress(KeyError):
                    company = self.registry.get(record.company_slug)
            company_slug = company.slug if company else (record.company_slug if record else None)
            plate_no = self.registry.normalize_plate(company, plate_no) if company else self.registry.normalize_plate_any(plate_no)
            timezone_name = self.registry.timezone_for(
                device_id=alarm.device_id,
                fleet_id=effective_fleet_id,
                slug=company_slug,
                fallback=self.settings.default_timezone,
            )
            occurred_at, temporal_resolution = self._resolve_alarm_occurred_at(
                alarm=alarm,
                occurred_at=occurred_at,
                received_at=received_at,
                timezone_name=timezone_name,
            )
            provider_event_key = _build_provider_event_key(
                company_slug=company_slug,
                device_id=alarm.device_id,
                category=alarm.category,
                raw_alarm_type=alarm.raw_alarm_type,
                raw_tp=alarm.raw_tp,
                raw_event_code=alarm.raw_event_code,
                occurred_at=occurred_at,
                start_at=alarm.start_at,
                end_at=alarm.end_at,
            )
            tolerance = timedelta(minutes=self.settings.anomaly_future_tolerance_minutes)
            temporal_valid = occurred_at is not None and occurred_at - received_at <= tolerance
            temporal_status = "accepted" if temporal_valid else "future_rejected"
            snapshot_date = to_local_date(occurred_at or received_at, timezone_name)
            plate_no = plate_no or (record.plate_no if record else None)
            fleet_id = effective_fleet_id
            driver_name = alarm.driver_name or (record.driver_name if record else None)
            if not record:
                record = DeviceRecord(device_id=alarm.device_id)
            previous_identity = (record.company_slug, record.fleet_id, record.plate_no)
            record.plate_no = plate_no or record.plate_no
            record.company_slug = company_slug or record.company_slug
            record.fleet_id = fleet_id or record.fleet_id
            record.driver_name = driver_name or record.driver_name
            record.last_seen_at = _max_datetime(record.last_seen_at, occurred_at)
            record.last_received_at = _max_datetime(record.last_received_at, received_at)
            if alarm.total_mileage_km is not None:
                if record.last_total_km is None or record.last_total_km > alarm.total_mileage_km or alarm.total_mileage_km >= record.last_total_km:
                    record.last_total_km = alarm.total_mileage_km
            session.add(record)
            if previous_identity != (record.company_slug, record.fleet_id, record.plate_no):
                self._propagate_company_assignment(
                    session,
                    device_id=alarm.device_id,
                    company_slug=record.company_slug,
                    plate_no=record.plate_no,
                    fleet_id=record.fleet_id,
                )

            raw_row = None
            if provider_event_key:
                raw_row = session.scalar(
                    select(HowenAlarmRaw).where(HowenAlarmRaw.provider_event_key == provider_event_key)
                )
            if not raw_row:
                raw_row = session.get(HowenAlarmRaw, alarm.guid)
            if not raw_row:
                raw_row = _match_existing_raw_alarm(
                    session,
                    device_id=alarm.device_id,
                    category=alarm.category,
                    raw_alarm_type=alarm.raw_alarm_type,
                    raw_tp=alarm.raw_tp,
                    raw_event_code=alarm.raw_event_code,
                    occurred_at=occurred_at,
                    start_at=alarm.start_at,
                    end_at=alarm.end_at,
                )
            if not raw_row:
                raw_row = HowenAlarmRaw(
                    guid=alarm.guid,
                    provider_event_key=provider_event_key,
                    company_slug=company_slug,
                    device_id=alarm.device_id,
                    fleet_id=fleet_id,
                    plate_no=plate_no,
                    source=source,
                    occurred_at=occurred_at,
                    received_at=received_at,
                    raw_alarm_type=alarm.raw_alarm_type,
                    raw_tp=alarm.raw_tp,
                    raw_event_code=alarm.raw_event_code,
                    classification_status=alarm.classification_status,
                    mapped_category=alarm.category,
                    mapping_source=alarm.mapping_source,
                )
                inserted_raw_row = True
            raw_row.company_slug = company_slug or raw_row.company_slug
            raw_row.provider_event_key = provider_event_key or raw_row.provider_event_key
            raw_row.device_id = alarm.device_id
            raw_row.fleet_id = fleet_id or raw_row.fleet_id
            raw_row.plate_no = plate_no or raw_row.plate_no
            raw_row.source = source if source == "harvest" or not raw_row.source else raw_row.source
            raw_row.occurred_at = occurred_at
            raw_row.received_at = _max_datetime(raw_row.received_at, received_at) or received_at
            raw_row.raw_alarm_type = alarm.raw_alarm_type
            raw_row.raw_tp = alarm.raw_tp
            raw_row.raw_event_code = alarm.raw_event_code
            raw_row.raw_event_time = alarm.raw_event_time
            raw_row.classification_status = alarm.classification_status
            raw_row.mapped_category = alarm.category
            raw_row.mapping_source = alarm.mapping_source
            raw_row.temporal_status = temporal_status
            raw_row.payload_json = json.dumps(alarm.raw, ensure_ascii=True)
            session.add(raw_row)

            if temporal_valid and alarm.classification_status == "classified_dms":
                existing_event = None
                if provider_event_key:
                    existing_event = session.scalar(
                        select(AlarmEvent).where(AlarmEvent.provider_event_key == provider_event_key)
                    )
                if not existing_event:
                    existing_event = session.get(AlarmEvent, alarm.guid)
                if not existing_event:
                    existing_event = _match_existing_alarm_event(
                        session,
                        device_id=alarm.device_id,
                        category=alarm.category,
                        raw_alarm_type=alarm.raw_alarm_type,
                        raw_tp=alarm.raw_tp,
                        raw_event_code=alarm.raw_event_code,
                        occurred_at=occurred_at,
                        start_at=alarm.start_at,
                        end_at=alarm.end_at,
                    )
                if not existing_event:
                    existing_event = AlarmEvent(
                        guid=alarm.guid,
                        provider_event_key=provider_event_key,
                        device_id=alarm.device_id,
                        occurred_at=occurred_at,
                        source=source,
                    )
                    inserted_alarm_event = True
                existing_event.provider_event_key = provider_event_key or existing_event.provider_event_key
                existing_event.plate_no = plate_no or existing_event.plate_no
                existing_event.company_slug = company_slug or existing_event.company_slug
                existing_event.fleet_id = fleet_id or existing_event.fleet_id
                existing_event.driver_name = driver_name or existing_event.driver_name
                existing_event.category = alarm.category
                existing_event.subtype = alarm.subtype
                existing_event.mapping_source = alarm.mapping_source
                existing_event.classification_status = alarm.classification_status
                existing_event.visibility_status = alarm.visibility_status
                existing_event.event_code = alarm.event_code
                existing_event.raw_alarm_type = alarm.raw_alarm_type
                existing_event.raw_tp = alarm.raw_tp
                existing_event.raw_event_code = alarm.raw_event_code
                existing_event.occurred_at = occurred_at
                existing_event.received_at = received_at
                existing_event.start_at = alarm.start_at
                existing_event.end_at = alarm.end_at
                existing_event.raw_event_time = alarm.raw_event_time
                existing_event.latitude = alarm.latitude
                existing_event.longitude = alarm.longitude
                existing_event.total_mileage_km = alarm.total_mileage_km
                existing_event.source = source if source == "harvest" or existing_event.source != "harvest" else existing_event.source
                # Keep only the normalized analytic columns here. The raw payload is
                # retained once in HowenAlarmRaw and linked by provider_event_key.
                existing_event.raw_payload = None
                session.add(existing_event)
                raw_ingest_result = "inserted_alarm_event" if inserted_alarm_event else "updated_alarm_event"
            elif not temporal_valid:
                raw_ingest_result = "future_rejected"
            elif alarm.classification_status == "classified_non_dms":
                raw_ingest_result = "kept_raw_only_non_dms"
            elif alarm.classification_status == "unmapped":
                raw_ingest_result = "kept_raw_only_unmapped"
            else:
                raw_ingest_result = "kept_raw_only"

            raw_row.ingest_result = raw_ingest_result
            if alarm.classification_status == "unmapped" or (
                alarm.classification_status == "classified_dms"
                and (alarm.mapping_source or "unknown") == "unknown"
            ):
                self._append_alarm_audit(
                    session,
                    guid=alarm.guid,
                    company_slug=company_slug,
                    device_id=alarm.device_id,
                    fleet_id=fleet_id,
                    plate_no=plate_no,
                    observed_at=occurred_at,
                    received_at=received_at,
                    raw_alarm_type=alarm.raw_alarm_type,
                    raw_tp=alarm.raw_tp,
                    raw_event_code=alarm.raw_event_code,
                    stage=f"mapping_review_{source}",
                    reason=alarm.classification_status or "unknown",
                    payload=alarm.raw,
                    provider_event_key=provider_event_key,
                )
            if temporal_resolution:
                self._append_alarm_audit(
                    session,
                    guid=alarm.guid,
                    company_slug=company_slug,
                    device_id=alarm.device_id,
                    fleet_id=fleet_id,
                    plate_no=plate_no,
                    observed_at=occurred_at,
                    received_at=received_at,
                    raw_alarm_type=alarm.raw_alarm_type,
                    raw_tp=alarm.raw_tp,
                    raw_event_code=alarm.raw_event_code,
                    stage=f"temporal_resolution_{source}",
                    reason=temporal_resolution,
                    payload=alarm.raw,
                    provider_event_key=provider_event_key,
                )
            if not temporal_valid:
                self._append_alarm_audit(
                    session,
                    guid=alarm.guid,
                    company_slug=company_slug,
                    device_id=alarm.device_id,
                    fleet_id=fleet_id,
                    plate_no=plate_no,
                    observed_at=occurred_at,
                    received_at=received_at,
                    raw_alarm_type=alarm.raw_alarm_type,
                    raw_tp=alarm.raw_tp,
                    raw_event_code=alarm.raw_event_code,
                    stage=f"{source}_alarm",
                    reason="future_timestamp",
                    payload=alarm.raw,
                    provider_event_key=provider_event_key,
                )

            state = session.get(IngestState, "global")
            if state:
                state.mode = "live"
                if source == "live":
                    state.connection_state = "connected"
                    state.last_message_at = _max_datetime(state.last_message_at, received_at)
                    state.last_cycle_received_at = _max_datetime(state.last_cycle_received_at, received_at)
                    state.last_live_alarm_message_at = _max_datetime(state.last_live_alarm_message_at, received_at)
                    state.last_error = None
                    if temporal_valid:
                        state.last_event_observed_at = _max_datetime(state.last_event_observed_at, occurred_at)
                        state.last_alarm_at = _max_datetime(state.last_alarm_at, occurred_at)
                        if alarm.classification_status == "classified_dms":
                            state.last_live_dms_at = _max_datetime(state.last_live_dms_at, occurred_at)
                        elif alarm.classification_status == "unmapped":
                            state.last_live_unmapped_at = _max_datetime(state.last_live_unmapped_at, occurred_at)
            session.commit()

        if not temporal_valid:
            await self._record_anomaly(
                source_type=f"{source}_alarm",
                device_id=alarm.device_id,
                company_slug=company_slug,
                received_at=received_at,
                raw_event_time=alarm.raw_event_time,
                reason="future_timestamp",
                payload=alarm.raw,
            )
            return {
                "provider_event_key": provider_event_key,
                "inserted_raw": inserted_raw_row,
                "inserted_alarm_event": False,
                "temporal_status": temporal_status,
                "ingest_result": raw_ingest_result,
            }
        self.mark_dirty()
        return {
            "provider_event_key": provider_event_key,
            "inserted_raw": inserted_raw_row,
            "inserted_alarm_event": inserted_alarm_event,
            "temporal_status": temporal_status,
            "ingest_result": raw_ingest_result,
        }

    def _resolve_alarm_occurred_at(
        self,
        *,
        alarm: NormalizedAlarm,
        occurred_at: datetime | None,
        received_at: datetime,
        timezone_name: str,
    ) -> tuple[datetime | None, str | None]:
        occurred_at = ensure_utc(occurred_at)
        tolerance = timedelta(minutes=self.settings.anomaly_future_tolerance_minutes)
        if occurred_at is None or occurred_at - received_at <= tolerance:
            return occurred_at, None

        payload = alarm.raw if isinstance(alarm.raw, dict) else {}
        fallback_candidates = (
            ("fallback_reportTime", _payload_value(payload, "reportTime")),
            ("fallback_endTime", _payload_value(payload, "endTime", "et")),
            ("fallback_startTime", _payload_value(payload, "startTime", "st")),
        )
        current_raw = (alarm.raw_event_time or "").strip()

        for reason, raw_value in fallback_candidates:
            candidate_text = (raw_value or "").strip()
            if not candidate_text or candidate_text == current_raw:
                continue
            candidate = parse_timestamp(candidate_text, timezone_name)
            if candidate is None:
                continue
            candidate = ensure_utc(candidate)
            if candidate is not None and candidate - received_at <= tolerance:
                return candidate, reason

        return occurred_at, None

    def _ensure_state_row(self) -> None:
        with self.session_factory() as session:
            if not session.get(IngestState, "global"):
                session.add(IngestState(key="global", mode="live", connection_state="starting"))
                session.commit()

    async def _set_state(self, *, connection_state: str, last_error: str | None) -> None:
        with self.session_factory() as session:
            state = session.get(IngestState, "global")
            if not state:
                state = IngestState(key="global")
                session.add(state)
            state.mode = "live"
            state.connection_state = connection_state
            state.last_error = last_error
            session.commit()
        self.mark_dirty()

    async def _validate_temporal_integrity(
        self,
        *,
        source_type: str,
        device_id: str | None,
        fleet_id: str | None,
        observed_at,
        raw_event_time: str | None,
        received_at,
        payload: dict[str, Any],
    ) -> bool:
        observed_at = ensure_utc(observed_at)
        received_at = ensure_utc(received_at) or utc_now()
        if observed_at is None:
            return False
        tolerance = timedelta(minutes=self.settings.anomaly_future_tolerance_minutes)
        if observed_at - received_at <= tolerance:
            return True
        company = self.registry.resolve_company(device_id=device_id, fleet_id=fleet_id)
        await self._record_anomaly(
            source_type=source_type,
            device_id=device_id,
            company_slug=company.slug if company else None,
            received_at=received_at,
            raw_event_time=raw_event_time,
            reason="future_timestamp",
            payload=payload,
        )
        return False

    async def _record_normalization_failure(
        self,
        *,
        source_type: str,
        payload: dict[str, Any],
        received_at,
    ) -> None:
        device_id = _payload_value(payload, "deviceID", "deviceno", "deviceid")
        fleet_id = _payload_value(payload, "fleetID", "fleetId", "fleetid")
        company = self.registry.resolve_company(device_id=device_id, fleet_id=fleet_id)
        raw_event_time = _payload_value(
            payload,
            "dtu",
            "reportTime",
            "st",
            "et",
        )
        await self._record_anomaly(
            source_type=source_type,
            device_id=device_id,
            company_slug=company.slug if company else None,
            received_at=received_at,
            raw_event_time=raw_event_time,
            reason="normalization_failed",
            payload=payload,
        )

    async def _record_ws_message_anomaly(
        self,
        *,
        action: str,
        payload: Any,
        received_at,
    ) -> None:
        normalized_payload = payload if isinstance(payload, dict) else {"action": action, "raw_payload": payload, "raw_type": type(payload).__name__}
        await self._record_anomaly(
            source_type="ws_message",
            device_id=None,
            company_slug=None,
            received_at=received_at,
            raw_event_time=None,
            reason="unexpected_message_shape",
            payload=normalized_payload,
        )

    async def _record_anomaly(
        self,
        *,
        source_type: str,
        device_id: str | None,
        company_slug: str | None,
        received_at,
        raw_event_time: str | None,
        reason: str,
        payload: dict[str, Any],
    ) -> None:
        with self.session_factory() as session:
            raw_alarm_type = _payload_alarm_type(payload)
            raw_tp = _payload_alarm_tp(payload)
            raw_event_code = _payload_alarm_event_code(payload)
            raw_plate_no = _payload_value(payload, "plateNo", "plateno", "plate")
            fleet_id = _payload_value(payload, "fleetID", "fleetId", "fleetid")
            company = None
            if company_slug:
                with suppress(KeyError):
                    company = self.registry.get(company_slug)
            plate_no = self.registry.normalize_plate(company, raw_plate_no) if company else self.registry.normalize_plate_any(raw_plate_no)
            session.add(
                IngestionAnomaly(
                    id=self._next_serial_id(session, "ingestion_anomalies"),
                    source_type=source_type,
                    device_id=device_id,
                    company_slug=company_slug,
                    received_at=received_at,
                    raw_event_time=raw_event_time,
                    reason=reason,
                    payload_json=json.dumps(payload, ensure_ascii=True),
                )
            )
            self._append_alarm_audit(
                session,
                guid=_payload_value(payload, "alarmID", "guid", "uuid"),
                company_slug=company_slug,
                device_id=device_id,
                fleet_id=fleet_id,
                plate_no=plate_no,
                observed_at=None,
                received_at=received_at,
                raw_alarm_type=raw_alarm_type,
                raw_tp=raw_tp,
                raw_event_code=raw_event_code,
                stage=source_type,
                reason=reason,
                payload=payload,
            )
            state = session.get(IngestState, "global")
            if state:
                state.last_anomaly_at = received_at
            session.commit()
        self.mark_dirty()

    def _append_alarm_audit(
        self,
        session,
        *,
        guid: str | None,
        company_slug: str | None,
        device_id: str | None,
        fleet_id: str | None,
        plate_no: str | None,
        observed_at,
        received_at,
        raw_alarm_type: str | None,
        raw_tp: str | None,
        raw_event_code: str | None,
        stage: str,
        reason: str,
        payload: dict[str, Any],
        provider_event_key: str | None = None,
    ) -> None:
        if guid:
            existing = session.scalar(
                select(AlarmEventAudit.id).where(
                    AlarmEventAudit.guid == guid,
                    AlarmEventAudit.stage == stage,
                    AlarmEventAudit.reason == reason,
                )
            )
            if existing:
                return
        bind = session.get_bind()
        audit_payload = {
            "id": self._next_serial_id(session, "alarm_event_audit"),
            "guid": guid,
            "provider_event_key": provider_event_key,
            "company_slug": company_slug,
            "device_id": device_id,
            "fleet_id": fleet_id,
            "plate_no": plate_no,
            "observed_at": ensure_utc(observed_at),
            "received_at": ensure_utc(received_at) or utc_now(),
            "raw_alarm_type": raw_alarm_type,
            "raw_tp": raw_tp,
            "raw_event_code": raw_event_code,
            "stage": stage,
            "reason": reason,
            "payload_json": "{}",
        }
        if bind is not None and bind.dialect.name.startswith("postgres"):
            session.execute(
                text(
                    """
                    INSERT INTO alarm_event_audit (
                        id,
                        guid,
                        provider_event_key,
                        company_slug,
                        device_id,
                        fleet_id,
                        plate_no,
                        observed_at,
                        received_at,
                        raw_alarm_type,
                        raw_tp,
                        raw_event_code,
                        stage,
                        reason,
                        payload_json
                    ) VALUES (
                        :id,
                        :guid,
                        :provider_event_key,
                        :company_slug,
                        :device_id,
                        :fleet_id,
                        :plate_no,
                        :observed_at,
                        :received_at,
                        :raw_alarm_type,
                        :raw_tp,
                        :raw_event_code,
                        :stage,
                        :reason,
                        :payload_json
                    )
                    ON CONFLICT DO NOTHING
                    """
                ),
                audit_payload,
            )
            return
        session.add(AlarmEventAudit(**audit_payload))

    def _next_serial_id(self, session, table_name: str) -> int | None:
        bind = session.get_bind()
        if bind is None or not bind.dialect.name.startswith("postgres"):
            return None
        return session.execute(
            text("SELECT nextval(pg_get_serial_sequence(:table_name, 'id'))"),
            {"table_name": table_name},
        ).scalar_one()

    def purge_retention(self, *, force: bool = False) -> dict[str, int]:
        now = utc_now()
        if not force and getattr(self, "_last_purge_at", None) and now - self._last_purge_at < timedelta(hours=1):
            return {"skipped": 1}
        live_cutoff = now - timedelta(days=self.settings.live_retention_days)
        anomaly_cutoff = now - timedelta(days=self.settings.anomaly_retention_days)
        counts: dict[str, int] = {}
        with self.session_factory() as session:
            raw_key = (
                select(HowenAlarmRaw.provider_event_key)
                .where(HowenAlarmRaw.guid == AlarmEventAudit.guid)
                .limit(1)
                .scalar_subquery()
            )
            linked_audits = session.execute(
                update(AlarmEventAudit)
                .where(
                    AlarmEventAudit.provider_event_key.is_(None),
                    AlarmEventAudit.guid.is_not(None),
                )
                .values(provider_event_key=raw_key)
            )
            counts["audits_linked"] = max(int(linked_audits.rowcount or 0), 0)

            compacted_audits = session.execute(
                update(AlarmEventAudit)
                .where(AlarmEventAudit.payload_json != "{}")
                .values(payload_json="{}")
            )
            counts["audits_compacted"] = max(int(compacted_audits.rowcount or 0), 0)

            raw_exists = (
                select(HowenAlarmRaw.guid)
                .where(
                    or_(
                        HowenAlarmRaw.guid == AlarmEvent.guid,
                        and_(
                            AlarmEvent.provider_event_key.is_not(None),
                            HowenAlarmRaw.provider_event_key == AlarmEvent.provider_event_key,
                        ),
                    )
                )
                .exists()
            )
            compacted_events = session.execute(
                update(AlarmEvent)
                .where(AlarmEvent.raw_payload.is_not(None), raw_exists)
                .values(raw_payload=None)
            )
            counts["analytic_payloads_compacted"] = max(int(compacted_events.rowcount or 0), 0)

            delete_statements = {
                "alarm_events_deleted": delete(AlarmEvent).where(AlarmEvent.occurred_at < live_cutoff),
                "raw_events_deleted": delete(HowenAlarmRaw).where(HowenAlarmRaw.received_at < live_cutoff),
                "mileage_readings_deleted": delete(MileageReading).where(MileageReading.recorded_at < live_cutoff),
                "mileage_observations_deleted": delete(MileageObservation).where(
                    MileageObservation.observed_at < live_cutoff
                ),
                "daily_snapshots_deleted": delete(DailyMileageSnapshot).where(DailyMileageSnapshot.observed_at < live_cutoff),
                "anomalies_deleted": delete(IngestionAnomaly).where(IngestionAnomaly.received_at < anomaly_cutoff),
                "audits_deleted": delete(AlarmEventAudit).where(AlarmEventAudit.received_at < anomaly_cutoff),
                "decisions_deleted": delete(ReconciliationReview).where(
                    ReconciliationReview.review_status.in_(("approved", "discarded")),
                    ReconciliationReview.decided_at.is_not(None),
                    ReconciliationReview.decided_at < anomaly_cutoff,
                ),
            }
            for key, statement in delete_statements.items():
                result = session.execute(statement)
                counts[key] = max(int(result.rowcount or 0), 0)
            session.commit()
        self._last_purge_at = now
        logger.info(
            "retention_completed live_days=%s decision_days=%s counts=%s",
            self.settings.live_retention_days,
            self.settings.anomaly_retention_days,
            counts,
        )
        return counts

    async def _purge_if_needed(self) -> None:
        await asyncio.to_thread(self.purge_retention)

    def _should_sync_devices(self) -> bool:
        if not self._last_device_sync_at:
            return True
        return utc_now() - self._last_device_sync_at >= timedelta(hours=24)

    def _resolve_backfill_device_ids(self, request: BackfillRequest) -> list[str]:
        if request.device_id:
            return [request.device_id]
        if request.company_slug:
            return self._list_company_device_ids(request.company_slug)
        return []

    def _list_company_device_ids(self, company_slug: str) -> list[str]:
        company = self.registry.get(company_slug)
        with self.session_factory() as db:
            devices = list(
                db.scalars(
                    select(DeviceRecord)
                    .where(DeviceRecord.record_source == "live")
                    .order_by(DeviceRecord.device_id)
                )
            )
        device_ids = [
            device.device_id
            for device in devices
            if self.registry.device_belongs(company, device.device_id, device.fleet_id)
        ]
        if not device_ids and company.device_ids:
            device_ids = list(company.device_ids)
        return sorted(set(device_ids))

    def _propagate_company_assignment(
        self,
        session,
        *,
        device_id: str | None,
        company_slug: str | None,
        plate_no: str | None,
        fleet_id: str | None,
    ) -> None:
        if not device_id or not company_slug:
            return
        for model in (HowenAlarmRaw, AlarmEvent, DailyMileageSnapshot):
            values: dict[str, Any] = {
                "company_slug": func.coalesce(model.company_slug, company_slug),
            }
            if plate_no:
                values["plate_no"] = plate_no
            if fleet_id:
                values["fleet_id"] = func.coalesce(model.fleet_id, fleet_id)
            session.execute(
                update(model)
                .where(model.device_id == device_id)
                .values(**values)
            )


def _max_datetime(left, right):
    left = ensure_utc(left)
    right = ensure_utc(right)
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def _merge_alarm_batch_metrics(target: AlarmBatchResult, payload: dict[str, Any] | None) -> None:
    if not payload:
        return
    for field_name in (
        "provider_rows",
        "prepared_rows",
        "raw_inserted",
        "raw_updated",
        "dms_inserted",
        "dms_updated",
        "duplicates",
        "non_dms",
        "unmapped",
        "temporal_rejected",
        "anomalies",
        "errors",
        "chunks_committed",
    ):
        setattr(target, field_name, getattr(target, field_name) + int(payload.get(field_name, 0) or 0))
    target.latest_observed_at = _max_datetime(
        target.latest_observed_at,
        parse_timestamp(payload.get("latest_observed_at")),
    )


def _payload_value(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    for key in ("payload", "basic", "detail", "ext", "location", "det", "mileage", "module"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            nested_value = _payload_value(nested, *keys)
            if nested_value:
                return nested_value
    return None


def _payload_alarm_type(payload: dict[str, Any]) -> str | None:
    return _payload_value(payload, "alarmTypeValue", "alarmType", "alarmtypeValue")


def _payload_alarm_tp(payload: dict[str, Any]) -> str | None:
    direct = _payload_value(payload, "tp")
    if direct:
        return direct
    alarm_detail = _payload_value(payload, "alarmvalue", "alarmDetail")
    if not alarm_detail:
        return None
    marker = "tp:"
    if marker not in alarm_detail:
        return None
    tail = alarm_detail.split(marker, 1)[1]
    digits = ""
    for char in tail:
        if char.isdigit():
            digits += char
            continue
        break
    return digits or None


def _payload_alarm_event_code(payload: dict[str, Any]) -> str | None:
    return _payload_value(payload, "ec", "alarmtype", "alarmType")


def _append_reason(current: str | None, reason: str) -> str:
    if not current:
        return reason
    reasons = [item for item in current.split(",") if item]
    if reason in reasons:
        return current
    reasons.append(reason)
    return ",".join(reasons)


def _date_range(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _mileage_report_device_id(row: dict[str, Any]) -> str | None:
    for key in ("deviceId", "deviceID", "deviceid", "deviceno", "deviceNo", "dtu"):
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip()
    for key in ("deviceName", "devicename", "name"):
        value = str(row.get(key) or "").strip()
        if "(" in value and value.endswith(")"):
            candidate = value.rsplit("(", 1)[-1][:-1].strip()
            if candidate:
                return candidate
    return None


def _mileage_report_plate(row: dict[str, Any]) -> str | None:
    for key in ("plateNo", "plateno", "plate", "vehicleNo"):
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip()
    for key in ("deviceName", "devicename", "name"):
        value = str(row.get(key) or "").strip()
        if not value:
            continue
        candidate = value.split("(", 1)[0].strip()
        if candidate:
            return candidate
    return None


def _mileage_report_daily_values(
    row: dict[str, Any],
    *,
    start_date: date,
    end_date: date,
) -> dict[date, tuple[float, Any]]:
    values: dict[date, tuple[float, Any]] = {}
    aliases: dict[str, date] = {}
    for day in _date_range(start_date, end_date):
        for alias in (
            day.isoformat(),
            day.strftime("%Y%m%d"),
            day.strftime("%m-%d"),
            day.strftime("%-m-%-d"),
            day.strftime("%m/%d"),
        ):
            aliases[alias] = day

    for key, raw_value in row.items():
        day = aliases.get(str(key).strip())
        if day is None:
            continue
        parsed = _parse_report_km(raw_value)
        if parsed is not None:
            values[day] = (parsed, raw_value)

    for nested_key in ("list", "rows", "items", "details", "dailyMileageList", "dataList"):
        nested_rows = row.get(nested_key)
        if not isinstance(nested_rows, list):
            continue
        for item in nested_rows:
            if not isinstance(item, dict):
                continue
            raw_date = item.get("date") or item.get("day") or item.get("statDate") or item.get("time")
            parsed_date = _parse_report_date(raw_date)
            if parsed_date is None or not (start_date <= parsed_date <= end_date):
                continue
            raw_value = (
                item.get("mileage")
                if item.get("mileage") is not None
                else item.get("dayMileage", item.get("value"))
            )
            parsed = _parse_report_km(raw_value)
            if parsed is not None:
                values[parsed_date] = (parsed, raw_value)
    return values


def _parse_report_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value or "").strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _parse_report_km(value: Any) -> float | None:
    if isinstance(value, dict):
        value = value.get("mileage") or value.get("dayMileage") or value.get("value")
    if value in (None, "", "-"):
        return None
    if isinstance(value, bool):
        return None
    try:
        normalized = str(value).replace(",", "").replace(" km", "").strip()
        parsed = float(normalized)
        # Howen serializes report cells without a decimal point in meters.
        # Explicit decimal values are already expressed in kilometres.
        if isinstance(value, int) or (
            isinstance(value, str)
            and normalized.lstrip("+-").isdigit()
        ):
            parsed /= 1_000.0
        return round(parsed, 3)
    except (TypeError, ValueError):
        return None


def _km_review_note(reason: str | None) -> str:
    messages = {
        "negative_day_km": "Howen devolvio una distancia diaria negativa. Debe aprobarse una reparacion o excluirse el vehiculo/dia.",
        "impossible_day_distance": "La distancia diaria supera el limite operativo configurado y no se publicara sin revision humana.",
        "source_disagreement": "El reporte diario de Howen difiere de una reparacion manual ya aprobada. Se conserva la decision vigente hasta nueva revision.",
    }
    return messages.get(reason, "El kilometraje no paso las validaciones operativas y requiere decision humana.")


def _normalize_event_key_timestamp(value: datetime | None) -> str:
    normalized = ensure_utc(value)
    if normalized is None:
        return "-"
    return normalized.replace(microsecond=0).isoformat()


def _event_key_token(value: str | None) -> str:
    return str(value or "").strip().lower()


def _build_provider_event_key(
    *,
    company_slug: str | None,
    device_id: str,
    category: str | None,
    raw_alarm_type: str | None,
    raw_tp: str | None,
    raw_event_code: str | None,
    occurred_at: datetime | None,
    start_at: datetime | None,
    end_at: datetime | None,
) -> str:
    parts = [
        _event_key_token(company_slug or "unknown"),
        _event_key_token(device_id),
        _event_key_token(category),
        _event_key_token(raw_alarm_type),
        _event_key_token(raw_tp),
        _event_key_token(raw_event_code),
        _normalize_event_key_timestamp(occurred_at),
        _normalize_event_key_timestamp(start_at),
        _normalize_event_key_timestamp(end_at),
    ]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def _raw_fuzzy_key(row: HowenAlarmRaw) -> tuple[Any, ...]:
    return (
        row.device_id,
        row.mapped_category,
        row.raw_alarm_type,
        row.raw_tp,
        row.raw_event_code,
        ensure_utc(row.occurred_at),
    )


def _event_fuzzy_key(row: AlarmEvent) -> tuple[Any, ...]:
    return (
        row.device_id,
        row.category,
        row.raw_alarm_type,
        row.raw_tp,
        row.raw_event_code,
        ensure_utc(row.occurred_at),
    )


def _bulk_upsert_rows(
    session: Any,
    model: Any,
    rows: list[dict[str, Any]],
    *,
    conflict_columns: list[str],
) -> None:
    if not rows:
        return
    dialect_name = session.get_bind().dialect.name
    if dialect_name.startswith("postgres"):
        statement = postgresql_insert(model).values(rows)
    elif dialect_name == "sqlite":
        statement = sqlite_insert(model).values(rows)
    else:
        session.execute(insert(model), rows)
        return
    update_columns = {
        column_name: getattr(statement.excluded, column_name)
        for column_name in rows[0]
        if column_name not in conflict_columns
    }
    statement = statement.on_conflict_do_update(
        index_elements=conflict_columns,
        set_=update_columns,
    )
    session.execute(statement)


def _match_existing_raw_alarm(
    session,
    *,
    device_id: str,
    category: str | None,
    raw_alarm_type: str | None,
    raw_tp: str | None,
    raw_event_code: str | None,
    occurred_at: datetime | None,
    start_at: datetime | None,
    end_at: datetime | None,
) -> HowenAlarmRaw | None:
    if occurred_at is None:
        return None
    return session.scalar(
        select(HowenAlarmRaw).where(
            HowenAlarmRaw.device_id == device_id,
            HowenAlarmRaw.mapped_category == category,
            HowenAlarmRaw.raw_alarm_type == raw_alarm_type,
            HowenAlarmRaw.raw_tp == raw_tp,
            HowenAlarmRaw.raw_event_code == raw_event_code,
            HowenAlarmRaw.occurred_at == occurred_at,
        )
    )


def _match_existing_alarm_event(
    session,
    *,
    device_id: str,
    category: str | None,
    raw_alarm_type: str | None,
    raw_tp: str | None,
    raw_event_code: str | None,
    occurred_at: datetime | None,
    start_at: datetime | None,
    end_at: datetime | None,
) -> AlarmEvent | None:
    if occurred_at is None:
        return None
    return session.scalar(
        select(AlarmEvent).where(
            AlarmEvent.device_id == device_id,
            AlarmEvent.category == category,
            AlarmEvent.raw_alarm_type == raw_alarm_type,
            AlarmEvent.raw_tp == raw_tp,
            AlarmEvent.raw_event_code == raw_event_code,
            AlarmEvent.occurred_at == occurred_at,
        )
    )
