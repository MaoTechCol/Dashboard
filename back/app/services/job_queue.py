from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import aliased

from app.core.time import ensure_utc, utc_now
from app.models import BackgroundJob


PRIORITY_HARVEST_CUT = 100
PRIORITY_REFRESH = 90
PRIORITY_COMPANY_PURGE = 80
PRIORITY_DATA_MAINTENANCE = 70
PRIORITY_BACKFILL = 50
PRIORITY_RECONCILIATION = 40
PRIORITY_HISTORICAL_REBUILD = 10


@dataclass(frozen=True)
class RetryJob(Exception):
    message: str
    retry_at: datetime | None = None


class JobQueue:
    def __init__(self, *, session_factory: Any, settings: Any) -> None:
        self.session_factory = session_factory
        self.settings = settings

    def enqueue(
        self,
        *,
        job_type: str,
        payload: dict[str, Any],
        priority: int,
        idempotency_key: str,
        company_slug: str | None = None,
        max_attempts: int | None = None,
        next_attempt_at: datetime | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        with self.session_factory() as session:
            existing = session.scalar(
                select(BackgroundJob).where(BackgroundJob.idempotency_key == idempotency_key)
            )
            if existing:
                result = self.serialize(existing)
                result["created"] = False
                return result

            job = BackgroundJob(
                id=uuid4().hex,
                job_type=job_type,
                company_slug=company_slug,
                priority=priority,
                status="queued",
                payload_json=json.dumps(payload, ensure_ascii=True, default=str),
                idempotency_key=idempotency_key,
                max_attempts=max_attempts or int(self.settings.worker_max_attempts),
                next_attempt_at=ensure_utc(next_attempt_at) or now,
            )
            session.add(job)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                existing = session.scalar(
                    select(BackgroundJob).where(BackgroundJob.idempotency_key == idempotency_key)
                )
                if existing:
                    result = self.serialize(existing)
                    result["created"] = False
                    return result
                raise
            session.refresh(job)
            result = self.serialize(job)
            result["created"] = True
            return result

    def enqueue_latest_harvest(
        self,
        *,
        company_slug: str,
        cut_at: datetime,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Keep at most one queued harvest per company, always for the newest cut."""
        requested_cut = ensure_utc(cut_at) or utc_now()
        now = utc_now()
        with self.session_factory() as session:
            idempotency_key = f"harvest:{company_slug}:{requested_cut.isoformat()}"
            exact = session.scalar(
                select(BackgroundJob).where(BackgroundJob.idempotency_key == idempotency_key)
            )
            if exact and exact.status == "failed":
                self._requeue_failed_job(exact, payload=payload, now=now)
                session.add(exact)
                session.commit()
                session.refresh(exact)
                result = self.serialize(exact)
                result["created"] = True
                result["requeued"] = True
                return result
            active = list(
                session.scalars(
                    select(BackgroundJob).where(
                        BackgroundJob.company_slug == company_slug,
                        BackgroundJob.job_type == "harvest_cut",
                        BackgroundJob.status.in_(("queued", "running")),
                    )
                )
            )
            candidates = [
                (row, self._payload_cut(row))
                for row in active
            ]
            covering = [
                (row, row_cut)
                for row, row_cut in candidates
                if row_cut is not None and row_cut >= requested_cut
            ]
            if covering:
                selected, _ = max(covering, key=lambda item: item[1])
                self._supersede_queued(
                    session,
                    [row for row, _ in candidates if row.status == "queued" and row.id != selected.id],
                    superseded_by=selected.id,
                    now=now,
                )
                session.commit()
                result = self.serialize(selected)
                result["created"] = False
                return result

            queued = [row for row, _ in candidates if row.status == "queued"]
            job = BackgroundJob(
                id=uuid4().hex,
                job_type="harvest_cut",
                company_slug=company_slug,
                priority=PRIORITY_HARVEST_CUT,
                status="queued",
                payload_json=json.dumps(payload, ensure_ascii=True, default=str),
                idempotency_key=idempotency_key,
                max_attempts=int(self.settings.worker_max_attempts),
                next_attempt_at=now,
            )
            self._supersede_queued(
                session,
                queued,
                superseded_by=job.id,
                now=now,
            )
            stale_refreshes = [
                row
                for row in session.scalars(
                    select(BackgroundJob).where(
                        BackgroundJob.company_slug == company_slug,
                        BackgroundJob.job_type == "refresh_snapshot",
                        BackgroundJob.status == "queued",
                    )
                )
                if (self._payload_cut(row) or requested_cut) <= requested_cut
            ]
            self._supersede_queued(
                session,
                stale_refreshes,
                superseded_by=job.id,
                now=now,
            )
            session.add(job)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                existing = session.scalar(
                    select(BackgroundJob).where(
                        BackgroundJob.idempotency_key == job.idempotency_key
                    )
                )
                if not existing:
                    raise
                result = self.serialize(existing)
                result["created"] = False
                return result
            session.refresh(job)
            result = self.serialize(job)
            result["created"] = True
            return result

    def enqueue_latest_refresh(
        self,
        *,
        company_slug: str,
        cut_at: datetime,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Reuse a covering harvest and collapse repeated manual refresh clicks."""
        requested_cut = ensure_utc(cut_at) or utc_now()
        now = utc_now()
        with self.session_factory() as session:
            idempotency_key = f"refresh:{company_slug}:{requested_cut.isoformat()}"
            exact = session.scalar(
                select(BackgroundJob).where(BackgroundJob.idempotency_key == idempotency_key)
            )
            if exact and exact.status == "failed":
                self._requeue_failed_job(exact, payload=payload, now=now)
                session.add(exact)
                session.commit()
                session.refresh(exact)
                result = self.serialize(exact)
                result["created"] = True
                result["requeued"] = True
                return result
            harvests = list(
                session.scalars(
                    select(BackgroundJob).where(
                        BackgroundJob.company_slug == company_slug,
                        BackgroundJob.job_type == "harvest_cut",
                        BackgroundJob.status.in_(("queued", "running")),
                    )
                )
            )
            harvest_candidates = [(row, self._payload_cut(row)) for row in harvests]
            covering_harvests = [
                (row, row_cut)
                for row, row_cut in harvest_candidates
                if row_cut is not None and row_cut >= requested_cut
            ]
            if covering_harvests:
                selected, _ = max(covering_harvests, key=lambda item: item[1])
                result = self.serialize(selected)
                result["created"] = False
                result["reused_for_refresh"] = True
                return result

            refreshes = list(
                session.scalars(
                    select(BackgroundJob).where(
                        BackgroundJob.company_slug == company_slug,
                        BackgroundJob.job_type == "refresh_snapshot",
                        BackgroundJob.status.in_(("queued", "running")),
                    )
                )
            )
            refresh_candidates = [(row, self._payload_cut(row)) for row in refreshes]
            covering_refreshes = [
                (row, row_cut)
                for row, row_cut in refresh_candidates
                if row_cut is not None and row_cut >= requested_cut
            ]
            if covering_refreshes:
                selected, _ = max(covering_refreshes, key=lambda item: item[1])
                result = self.serialize(selected)
                result["created"] = False
                return result

            queued = [row for row in refreshes if row.status == "queued"]
            job = BackgroundJob(
                id=uuid4().hex,
                job_type="refresh_snapshot",
                company_slug=company_slug,
                priority=PRIORITY_REFRESH,
                status="queued",
                payload_json=json.dumps(payload, ensure_ascii=True, default=str),
                idempotency_key=idempotency_key,
                max_attempts=int(self.settings.worker_max_attempts),
                next_attempt_at=now,
            )
            self._supersede_queued(session, queued, superseded_by=job.id, now=now)
            session.add(job)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                existing = session.scalar(
                    select(BackgroundJob).where(
                        BackgroundJob.idempotency_key == job.idempotency_key
                    )
                )
                if not existing:
                    raise
                result = self.serialize(existing)
                result["created"] = False
                return result
            session.refresh(job)
            result = self.serialize(job)
            result["created"] = True
            return result

    def compact_redundant_jobs(self) -> dict[str, int]:
        """Remove stale queued cuts and refreshes left by an older scheduler."""
        now = utc_now()
        compacted = {"harvest_cut": 0, "refresh_snapshot": 0}
        with self.session_factory() as session:
            queued = list(
                session.scalars(
                    select(BackgroundJob).where(
                        BackgroundJob.status == "queued",
                        BackgroundJob.job_type.in_(("harvest_cut", "refresh_snapshot")),
                    )
                )
            )
            groups: dict[tuple[str, str], list[BackgroundJob]] = {}
            for row in queued:
                groups.setdefault((row.company_slug or "", row.job_type), []).append(row)
            for (_, job_type), rows in groups.items():
                if len(rows) <= 1:
                    continue
                keep = max(
                    rows,
                    key=lambda row: (self._payload_cut(row) or ensure_utc(row.created_at) or now),
                )
                redundant = [row for row in rows if row.id != keep.id]
                self._supersede_queued(
                    session,
                    redundant,
                    superseded_by=keep.id,
                    now=now,
                )
                compacted[job_type] += len(redundant)
            session.commit()
        return compacted

    def claim(
        self,
        *,
        worker_id: str,
        job_types: set[str] | None = None,
        defer_while_job_types_active: set[str] | None = None,
    ) -> BackgroundJob | None:
        now = utc_now()
        lease_expires_at = now + timedelta(seconds=max(int(self.settings.worker_lease_seconds), 30))
        with self.session_factory() as session:
            purge_job = aliased(BackgroundJob)
            companies_being_purged = select(purge_job.company_slug).where(
                purge_job.job_type == "company_purge",
                purge_job.status.in_(("queued", "running")),
                purge_job.company_slug.is_not(None),
            )
            if defer_while_job_types_active:
                blocking_job = session.scalar(
                    select(BackgroundJob.id)
                    .where(
                        BackgroundJob.job_type.in_(defer_while_job_types_active),
                        BackgroundJob.status.in_(("queued", "running")),
                        or_(
                            BackgroundJob.company_slug.is_(None),
                            BackgroundJob.company_slug.not_in(companies_being_purged),
                        ),
                    )
                    .limit(1)
                )
                if blocking_job is not None:
                    return None

            query = select(BackgroundJob).where(
                BackgroundJob.attempts < BackgroundJob.max_attempts,
                or_(
                    and_(
                        BackgroundJob.status == "queued",
                        BackgroundJob.next_attempt_at <= now,
                    ),
                    and_(
                        BackgroundJob.status == "running",
                        BackgroundJob.lease_expires_at.is_not(None),
                        BackgroundJob.lease_expires_at < now,
                    ),
                ),
                or_(
                    BackgroundJob.job_type == "company_purge",
                    BackgroundJob.company_slug.is_(None),
                    BackgroundJob.company_slug.not_in(companies_being_purged),
                ),
            )
            if job_types:
                query = query.where(BackgroundJob.job_type.in_(job_types))
            job = session.scalar(
                query
                .order_by(BackgroundJob.priority.desc(), BackgroundJob.created_at.asc())
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if not job:
                return None
            if job.job_type == "company_purge" and job.company_slug:
                healthy_writer = session.scalar(
                    select(BackgroundJob.id).where(
                        BackgroundJob.id != job.id,
                        BackgroundJob.company_slug == job.company_slug,
                        BackgroundJob.status == "running",
                        BackgroundJob.lease_expires_at.is_not(None),
                        BackgroundJob.lease_expires_at >= now,
                    ).limit(1)
                )
                if healthy_writer is not None:
                    job.next_attempt_at = now + timedelta(seconds=2)
                    session.add(job)
                    session.commit()
                    return None
                self._cancel_for_company_purge(
                    session,
                    company_slug=job.company_slug,
                    purge_job_id=job.id,
                    now=now,
                )
            job.status = "running"
            job.lease_owner = worker_id
            job.lease_expires_at = lease_expires_at
            job.heartbeat_at = now
            job.started_at = job.started_at or now
            job.finished_at = None
            job.last_error = None
            job.result_json = None
            job.attempts = int(job.attempts or 0) + 1
            session.add(job)
            session.commit()
            session.refresh(job)
            session.expunge(job)
            return job

    def company_purge_pending(self, *, company_slug: str) -> bool:
        with self.session_factory() as session:
            return session.scalar(
                select(BackgroundJob.id).where(
                    BackgroundJob.company_slug == company_slug,
                    BackgroundJob.job_type == "company_purge",
                    BackgroundJob.status.in_(("queued", "running")),
                ).limit(1)
            ) is not None

    def has_active_jobs(self, *, job_types: set[str]) -> bool:
        if not job_types:
            return False
        with self.session_factory() as session:
            return session.scalar(
                select(BackgroundJob.id)
                .where(
                    BackgroundJob.job_type.in_(job_types),
                    BackgroundJob.status.in_(("queued", "running")),
                )
                .limit(1)
            ) is not None

    def heartbeat(self, *, job_id: str, worker_id: str) -> bool:
        now = utc_now()
        with self.session_factory() as session:
            job = session.get(BackgroundJob, job_id)
            if not job or job.status != "running" or job.lease_owner != worker_id:
                return False
            job.heartbeat_at = now
            job.lease_expires_at = now + timedelta(seconds=max(int(self.settings.worker_lease_seconds), 30))
            session.add(job)
            session.commit()
            return True

    def complete(self, *, job_id: str, worker_id: str, result: dict[str, Any] | None = None) -> None:
        with self.session_factory() as session:
            job = session.get(BackgroundJob, job_id)
            if not job or job.lease_owner != worker_id:
                return
            job.status = "succeeded"
            job.result_json = json.dumps(result or {}, ensure_ascii=True, default=str)
            job.last_error = None
            job.finished_at = utc_now()
            job.heartbeat_at = job.finished_at
            job.lease_owner = None
            job.lease_expires_at = None
            session.add(job)
            session.commit()

    def fail(
        self,
        *,
        job_id: str,
        worker_id: str,
        error: str,
        retry_at: datetime | None = None,
    ) -> None:
        now = utc_now()
        with self.session_factory() as session:
            job = session.get(BackgroundJob, job_id)
            if not job or job.lease_owner != worker_id:
                return
            can_retry = int(job.attempts or 0) < int(job.max_attempts or 1)
            if can_retry:
                exponent = max(int(job.attempts or 1) - 1, 0)
                fallback_delay = min(
                    int(self.settings.worker_retry_base_seconds) * (2**exponent),
                    int(self.settings.worker_retry_max_seconds),
                )
                job.status = "queued"
                job.next_attempt_at = ensure_utc(retry_at) or (now + timedelta(seconds=fallback_delay))
                job.finished_at = None
            else:
                job.status = "failed"
                job.finished_at = now
            job.last_error = error[:4000]
            job.heartbeat_at = now
            job.lease_owner = None
            job.lease_expires_at = None
            session.add(job)
            session.commit()

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self.session_factory() as session:
            job = session.get(BackgroundJob, job_id)
            return self.serialize(job) if job else None

    def list_recent(
        self,
        *,
        company_slug: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with self.session_factory() as session:
            query = select(BackgroundJob)
            if company_slug:
                query = query.where(BackgroundJob.company_slug == company_slug)
            if status:
                query = query.where(BackgroundJob.status == status)
            rows = session.scalars(
                query.order_by(BackgroundJob.created_at.desc()).limit(max(1, min(limit, 200)))
            )
            return [self.serialize(row) for row in rows]

    def summary(self) -> dict[str, Any]:
        now = utc_now()
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        with self.session_factory() as session:
            rows = list(
                session.scalars(
                    select(BackgroundJob)
                    .where(BackgroundJob.status.in_(("queued", "running", "failed")))
                    .order_by(BackgroundJob.priority.desc(), BackgroundJob.created_at.asc())
                )
            )

        queued = [row for row in rows if row.status == "queued"]
        running = [row for row in rows if row.status == "running"]
        failed = [
            row
            for row in rows
            if row.status == "failed" and ensure_utc(row.finished_at or row.updated_at or row.created_at) >= month_start
        ]
        healthy_running = [
            row
            for row in running
            if row.lease_expires_at and ensure_utc(row.lease_expires_at) >= now
        ]
        return {
            "queued": len(queued),
            "running": len(running),
            "failed": len(failed),
            "healthy_running": len(healthy_running),
            "stale_running": len(running) - len(healthy_running),
            "highest_priority_queued": max((row.priority for row in queued), default=None),
            "last_heartbeat_at": max(
                (ensure_utc(row.heartbeat_at) for row in running if row.heartbeat_at),
                default=None,
            ),
            "active": [self.serialize(row) for row in running[:10]],
        }

    def requeue_transient_harvests(self) -> int:
        """Repair harvest jobs incorrectly completed on a transient engine state."""
        transient_statuses = {
            "bootstrap_running",
            "failed",
            "maintenance",
            "partial",
            "queued",
            "rate_limited",
            "running",
        }
        now = utc_now()
        recovered = 0
        with self.session_factory() as session:
            rows = list(
                session.scalars(
                    select(BackgroundJob).where(
                        BackgroundJob.job_type == "harvest_cut",
                        BackgroundJob.status == "succeeded",
                        BackgroundJob.result_json.is_not(None),
                    )
                )
            )
            for job in rows:
                try:
                    result = json.loads(job.result_json or "{}")
                except json.JSONDecodeError:
                    continue
                result_status = str(result.get("status") or "").strip().lower() if isinstance(result, dict) else ""
                if result_status not in transient_statuses:
                    continue
                job.status = "queued"
                job.attempts = 0
                job.next_attempt_at = now
                job.lease_owner = None
                job.lease_expires_at = None
                job.heartbeat_at = None
                job.started_at = None
                job.finished_at = None
                job.last_error = f"Recovered transient harvest result: {result_status}"
                session.add(job)
                recovered += 1
            session.commit()
        return recovered

    @classmethod
    def _payload_cut(cls, job: BackgroundJob) -> datetime | None:
        value = cls.payload(job).get("cut_at")
        if isinstance(value, datetime):
            return ensure_utc(value)
        if not value:
            return None
        try:
            return ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
        except ValueError:
            return None

    @staticmethod
    def _supersede_queued(
        session: Any,
        rows: list[BackgroundJob],
        *,
        superseded_by: str,
        now: datetime,
    ) -> None:
        for row in rows:
            if row.status != "queued":
                continue
            row.status = "succeeded"
            row.result_json = json.dumps(
                {"status": "superseded", "superseded_by": superseded_by},
                ensure_ascii=True,
            )
            row.last_error = None
            row.finished_at = now
            row.heartbeat_at = now
            row.lease_owner = None
            row.lease_expires_at = None
            session.add(row)

    @staticmethod
    def _cancel_for_company_purge(
        session: Any,
        *,
        company_slug: str,
        purge_job_id: str,
        now: datetime,
    ) -> None:
        rows = list(
            session.scalars(
                select(BackgroundJob).where(
                    BackgroundJob.id != purge_job_id,
                    BackgroundJob.company_slug == company_slug,
                    or_(
                        BackgroundJob.status == "queued",
                        and_(
                            BackgroundJob.status == "running",
                            or_(
                                BackgroundJob.lease_expires_at.is_(None),
                                BackgroundJob.lease_expires_at < now,
                            ),
                        ),
                    ),
                )
            )
        )
        for row in rows:
            row.status = "succeeded"
            row.result_json = json.dumps(
                {
                    "status": "cancelled_by_company_purge",
                    "purge_job_id": purge_job_id,
                },
                ensure_ascii=True,
            )
            row.last_error = None
            row.finished_at = now
            row.heartbeat_at = now
            row.lease_owner = None
            row.lease_expires_at = None
            session.add(row)

    @staticmethod
    def _requeue_failed_job(
        job: BackgroundJob,
        *,
        payload: dict[str, Any],
        now: datetime,
    ) -> None:
        job.status = "queued"
        job.payload_json = json.dumps(payload, ensure_ascii=True, default=str)
        job.attempts = 0
        job.next_attempt_at = now
        job.started_at = None
        job.finished_at = None
        job.heartbeat_at = None
        job.lease_owner = None
        job.lease_expires_at = None
        job.last_error = None
        job.result_json = None

    @staticmethod
    def payload(job: BackgroundJob) -> dict[str, Any]:
        try:
            value = json.loads(job.payload_json or "{}")
        except json.JSONDecodeError:
            return {}
        return value if isinstance(value, dict) else {}

    @staticmethod
    def serialize(job: BackgroundJob) -> dict[str, Any]:
        result: dict[str, Any] | None = None
        if job.result_json:
            try:
                parsed = json.loads(job.result_json)
                result = parsed if isinstance(parsed, dict) else {"value": parsed}
            except json.JSONDecodeError:
                result = None
        return {
            "job_id": job.id,
            "job_type": job.job_type,
            "company_slug": job.company_slug,
            "priority": job.priority,
            "status": job.status,
            "attempts": job.attempts,
            "max_attempts": job.max_attempts,
            "lease_owner": job.lease_owner,
            "lease_expires_at": job.lease_expires_at,
            "heartbeat_at": job.heartbeat_at,
            "next_attempt_at": job.next_attempt_at,
            "last_error": job.last_error,
            "result": result,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
            "created_at": job.created_at,
            "updated_at": job.updated_at,
        }
