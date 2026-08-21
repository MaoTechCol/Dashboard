from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
from contextlib import suppress
from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import select

from app.bootstrap import AppContext, build_context
from app.core.systemd import memory_monitor_loop, notify_systemd, watchdog_loop
from app.core.time import ensure_utc
from app.models import CompanyHistoricalRebuildJob, IngestState
from app.schemas import BackfillRequest, HistoricalRebuildRequest, KmRepairRequest
from app.services.job_queue import (
    PRIORITY_HISTORICAL_REBUILD,
    JobQueue,
    RetryJob,
)


logger = logging.getLogger("dashboard.worker")


HARVEST_JOB_TYPES = {"harvest_cut", "refresh_snapshot"}
MAINTENANCE_JOB_TYPES = {
    "historical_rebuild",
    "backfill",
    "company_purge",
    "reconciliation",
    "replay_status_anomalies",
    "km_repair",
    "review_bulk_decision",
    "purge_mock",
}


class DashboardWorker:
    def __init__(self, *, context: AppContext) -> None:
        self.context = context
        self.queue: JobQueue = context.jobs
        self.worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:8]}"
        self._stop = asyncio.Event()

    async def run(self) -> None:
        logger.info("worker_start worker_id=%s", self.worker_id)
        await asyncio.to_thread(self._recover_stale_maintenance)
        recovered_harvests = await asyncio.to_thread(self.queue.requeue_transient_harvests)
        if recovered_harvests:
            logger.warning("worker_transient_harvests_requeued count=%s", recovered_harvests)
        compacted = await asyncio.to_thread(self.queue.compact_redundant_jobs)
        if any(compacted.values()):
            logger.warning("worker_redundant_jobs_compacted counts=%s", compacted)
        await self.context.ingestion.start(
            include_harvest_scheduler=False,
            include_realtime_publisher=False,
            resume_historical_rebuilds=False,
        )
        await asyncio.to_thread(self._enqueue_orphaned_rebuilds)
        watchdog_task = asyncio.create_task(watchdog_loop(self._stop), name="worker-systemd-watchdog")
        memory_monitor_task = asyncio.create_task(
            memory_monitor_loop(
                self._stop,
                role=self.context.settings.process_role,
                warning_mb=self.context.settings.memory_warning_mb,
                critical_mb=self.context.settings.memory_critical_mb,
                interval_seconds=self.context.settings.memory_monitor_interval_seconds,
            ),
            name="worker-memory-monitor",
        )
        notify_systemd("READY=1\nSTATUS=Worker disponible")
        scheduler_task = asyncio.create_task(self._scheduler_loop(), name="worker-scheduler")
        harvest_consumer_tasks = tuple(
            asyncio.create_task(
                self._consumer_loop(
                    name=f"harvest-{index + 1}",
                    job_types=HARVEST_JOB_TYPES,
                ),
                name=f"worker-harvest-{index + 1}",
            )
            for index in range(max(int(self.context.settings.worker_harvest_concurrency), 1))
        )
        maintenance_consumer_task = asyncio.create_task(
            self._consumer_loop(
                name="maintenance",
                job_types=MAINTENANCE_JOB_TYPES,
                defer_while_job_types_active=HARVEST_JOB_TYPES,
            ),
            name="worker-maintenance",
        )
        stop_task = asyncio.create_task(self._stop.wait(), name="worker-stop")
        critical_tasks = (
            scheduler_task,
            *harvest_consumer_tasks,
            maintenance_consumer_task,
            *self.context.ingestion.critical_runtime_tasks(),
        )
        try:
            done, _ = await asyncio.wait(
                (*critical_tasks, stop_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._stop.is_set():
                return
            for task in done:
                if task is stop_task:
                    continue
                exception = task.exception()
                if exception is not None:
                    raise exception
                raise RuntimeError(f"Critical worker task stopped unexpectedly: {task.get_name()}")
        finally:
            notify_systemd("STOPPING=1")
            for task in critical_tasks:
                task.cancel()
            watchdog_task.cancel()
            memory_monitor_task.cancel()
            stop_task.cancel()
            await asyncio.gather(
                *critical_tasks,
                watchdog_task,
                memory_monitor_task,
                stop_task,
                return_exceptions=True,
            )
            await self.context.ingestion.stop()
            logger.info("worker_stop worker_id=%s", self.worker_id)

    def stop(self) -> None:
        self._stop.set()

    def _recover_stale_maintenance(self) -> None:
        summary = self.queue.summary()
        if int(summary.get("healthy_running") or 0) > 0:
            return
        with self.context.session_factory() as session:
            state = session.get(IngestState, "global")
            if not state or not state.maintenance_mode:
                return
            stale_reason = state.maintenance_reason
            state.maintenance_mode = False
            state.maintenance_reason = None
            state.maintenance_started_at = None
            session.add(state)
            session.commit()
        logger.warning("worker_stale_maintenance_cleared reason=%s", stale_reason)

    async def _scheduler_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._enqueue_due_harvests()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("worker_scheduler_failed")
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=max(int(self.context.settings.worker_scheduler_interval_seconds), 5),
                )
            except asyncio.TimeoutError:
                continue

    async def _enqueue_due_harvests(self) -> None:
        due_cuts = await asyncio.to_thread(self.context.ingestion.due_harvest_cuts)
        queued_count = 0
        for company_slug, cut_at in due_cuts:
            cut_iso = cut_at.isoformat()
            result = await asyncio.to_thread(
                self.queue.enqueue_latest_harvest,
                company_slug=company_slug,
                cut_at=cut_at,
                payload={"company_slug": company_slug, "cut_at": cut_iso, "force": False},
            )
            if result.get("created"):
                queued_count += 1
        if queued_count:
            logger.info("worker_harvests_enqueued count=%s due=%s", queued_count, len(due_cuts))

    def _enqueue_orphaned_rebuilds(self) -> None:
        """Move rebuilds left by the pre-worker process into the durable queue."""
        pending: list[dict[str, Any]] = []
        with self.context.session_factory() as session:
            rows = list(
                session.scalars(
                    select(CompanyHistoricalRebuildJob)
                    .where(CompanyHistoricalRebuildJob.status.in_(("queued", "running")))
                    .order_by(CompanyHistoricalRebuildJob.created_at.asc())
                )
            )
            for row in rows:
                if row.status == "running":
                    row.status = "queued"
                    row.phase = "queued"
                    row.current_device_id = None
                    session.add(row)
                pending.append(
                    {
                        "id": row.id,
                        "company_slug": row.company_slug,
                        "start_date": row.start_date,
                        "end_date": row.end_date,
                    }
                )
            session.commit()

        for row in pending:
            request = HistoricalRebuildRequest(
                company_slug=str(row["company_slug"]),
                start_date=row["start_date"],
                end_date=row["end_date"],
                publish_snapshot=True,
                maintenance=False,
            )
            self.queue.enqueue(
                job_type="historical_rebuild",
                payload={
                    "request": request.model_dump(mode="json"),
                    "rebuild_job_id": row["id"],
                },
                priority=PRIORITY_HISTORICAL_REBUILD,
                idempotency_key=f"historical_rebuild:recovered:{row['id']}",
                company_slug=str(row["company_slug"]),
            )

    async def _consumer_loop(
        self,
        *,
        name: str,
        job_types: set[str],
        defer_while_job_types_active: set[str] | None = None,
    ) -> None:
        poll_seconds = max(float(self.context.settings.worker_poll_interval_seconds), 0.25)
        consumer_worker_id = f"{self.worker_id}:{name}"
        while not self._stop.is_set():
            job = await asyncio.to_thread(
                self.queue.claim,
                worker_id=consumer_worker_id,
                job_types=job_types,
                defer_while_job_types_active=defer_while_job_types_active,
            )
            if not job:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=poll_seconds)
                except asyncio.TimeoutError:
                    continue
                continue

            heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(job.id, worker_id=consumer_worker_id),
                name=f"heartbeat-{job.id}",
            )
            logger.info(
                "worker_job_claimed lane=%s job_id=%s job_type=%s company=%s priority=%s attempt=%s",
                name,
                job.id,
                job.job_type,
                job.company_slug,
                job.priority,
                job.attempts,
            )
            try:
                result = await self._execute(job.job_type, self.queue.payload(job))
            except asyncio.CancelledError:
                raise
            except RetryJob as exc:
                await asyncio.to_thread(
                    self.queue.fail,
                    job_id=job.id,
                    worker_id=consumer_worker_id,
                    error=exc.message,
                    retry_at=exc.retry_at,
                )
                logger.warning(
                    "worker_job_retry job_id=%s job_type=%s error=%s",
                    job.id,
                    job.job_type,
                    exc.message,
                )
            except Exception as exc:
                await asyncio.to_thread(
                    self.queue.fail,
                    job_id=job.id,
                    worker_id=consumer_worker_id,
                    error=str(exc),
                )
                logger.exception(
                    "worker_job_failed job_id=%s job_type=%s",
                    job.id,
                    job.job_type,
                )
            else:
                await asyncio.to_thread(
                    self.queue.complete,
                    job_id=job.id,
                    worker_id=consumer_worker_id,
                    result=result,
                )
                logger.info("worker_job_succeeded job_id=%s job_type=%s", job.id, job.job_type)
            finally:
                heartbeat_task.cancel()
                with suppress(asyncio.CancelledError):
                    await heartbeat_task

    async def _heartbeat_loop(self, job_id: str, *, worker_id: str) -> None:
        interval = max(int(self.context.settings.worker_heartbeat_seconds), 5)
        while True:
            await asyncio.sleep(interval)
            owned = await asyncio.to_thread(
                self.queue.heartbeat,
                job_id=job_id,
                worker_id=worker_id,
            )
            if not owned:
                return

    async def _execute(self, job_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.context.registry.reload()
        if job_type == "harvest_cut":
            cut_at = _parse_datetime(payload.get("cut_at"))
            result = await self.context.ingestion.run_harvest_cut(
                company_slug=str(payload["company_slug"]),
                cut_at=cut_at,
                force=bool(payload.get("force", False)),
            )
            return self._require_completed_harvest(result)
        if job_type == "refresh_snapshot":
            company_slug = str(payload["company_slug"])
            cut_at = _parse_datetime(payload.get("cut_at"))
            if await asyncio.to_thread(
                self.context.ingestion.is_cut_superseded,
                company_slug=company_slug,
                cut_at=cut_at,
            ):
                return {
                    "status": "superseded",
                    "company_slug": company_slug,
                    "cut_at": cut_at.isoformat(),
                    "reason": "A newer cut is already published",
                }
            result = await self.context.ingestion.run_harvest_cut(
                company_slug=company_slug,
                cut_at=cut_at,
                force=True,
            )
            return self._require_completed_harvest(result)
        if job_type == "historical_rebuild":
            request = HistoricalRebuildRequest.model_validate(payload["request"])
            result = await self.context.ingestion.rebuild_historical_window(
                request,
                rebuild_job_id=int(payload["rebuild_job_id"]),
            )
            if result.get("status") == "queued":
                raise RetryJob(
                    str(result.get("message") or "Reconstruccion diferida por el proveedor"),
                    _parse_optional_datetime(result.get("next_retry_at")),
                )
            return result
        if job_type == "backfill":
            result = await self.context.ingestion.backfill_historical(
                BackfillRequest.model_validate(payload["request"])
            )
            return result
        if job_type == "company_purge":
            company_slug = str(payload["company_slug"])
            result = await self.context.ingestion.purge_company_operational_data(company_slug=company_slug)
            self.context.auth.delete_company_users(company_slug=company_slug)
            self.context.registry.delete_company(slug=company_slug)
            self.context.ingestion.mark_dirty()
            return result
        if job_type == "reconciliation":
            return await self.context.dashboard.process_reconciliation_job(str(payload["reconciliation_job_id"]))
        if job_type == "replay_status_anomalies":
            result = await self.context.ingestion.replay_status_anomalies()
            self.context.ingestion.mark_dirty()
            return result
        if job_type == "km_repair":
            result = await asyncio.to_thread(
                self.context.dashboard.repair_km,
                KmRepairRequest.model_validate(payload["request"]),
            )
            self.context.ingestion.mark_dirty()
            return result
        if job_type == "review_bulk_decision":
            result = await asyncio.to_thread(
                self.context.dashboard.decide_reconciliation_reviews_bulk,
                review_ids=[int(review_id) for review_id in payload.get("review_ids", [])],
                action=str(payload["action"]),
                decided_by=str(payload["decided_by"]),
                note=payload.get("note"),
            )
            self.context.ingestion.mark_dirty()
            return result
        if job_type == "purge_mock":
            result = await asyncio.to_thread(self.context.dashboard.purge_mock_legacy)
            self.context.ingestion.mark_dirty()
            return result
        raise ValueError(f"Unsupported background job type: {job_type}")

    @staticmethod
    def _require_completed_harvest(result: dict[str, Any]) -> dict[str, Any]:
        result_status = str(result.get("status") or "").strip().lower()
        if result_status in {
            "bootstrap_running",
            "failed",
            "maintenance",
            "partial",
            "queued",
            "rate_limited",
            "running",
        }:
            message = str(result.get("error_message") or f"Harvest ended in {result_status}")
            raise RetryJob(message, _parse_optional_datetime(result.get("next_retry_at")))
        return result


def _parse_datetime(value: Any) -> datetime:
    parsed = _parse_optional_datetime(value)
    if parsed is None:
        raise ValueError("Job payload is missing a valid datetime")
    return parsed


def _parse_optional_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return ensure_utc(value)
    if not value:
        return None
    try:
        return ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


async def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    context = build_context(seed_users=False)
    worker = DashboardWorker(context=context)
    loop = asyncio.get_running_loop()
    for signal_name in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(signal_name, worker.stop)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(_main())
