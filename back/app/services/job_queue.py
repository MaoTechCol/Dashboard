from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError

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
                return self.serialize(existing)

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
                    return self.serialize(existing)
                raise
            session.refresh(job)
            return self.serialize(job)

    def claim(self, *, worker_id: str) -> BackgroundJob | None:
        now = utc_now()
        lease_expires_at = now + timedelta(seconds=max(int(self.settings.worker_lease_seconds), 30))
        with self.session_factory() as session:
            job = session.scalar(
                select(BackgroundJob)
                .where(
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
                )
                .order_by(BackgroundJob.priority.desc(), BackgroundJob.created_at.asc())
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if not job:
                return None
            job.status = "running"
            job.lease_owner = worker_id
            job.lease_expires_at = lease_expires_at
            job.heartbeat_at = now
            job.started_at = job.started_at or now
            job.finished_at = None
            job.last_error = None
            job.attempts = int(job.attempts or 0) + 1
            session.add(job)
            session.commit()
            session.refresh(job)
            session.expunge(job)
            return job

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
        failed = [row for row in rows if row.status == "failed"]
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
