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
from app.core.time import ensure_utc
from app.models import CompanyHistoricalRebuildJob
from app.schemas import BackfillRequest, HistoricalRebuildRequest, KmRepairRequest
from app.services.job_queue import (
    PRIORITY_HARVEST_CUT,
    PRIORITY_HISTORICAL_REBUILD,
    JobQueue,
    RetryJob,
)


logger = logging.getLogger("dashboard.worker")


class DashboardWorker:
    def __init__(self, *, context: AppContext) -> None:
        self.context = context
        self.queue: JobQueue = context.jobs
        self.worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:8]}"
        self._stop = asyncio.Event()

    async def run(self) -> None:
        logger.info("worker_start worker_id=%s", self.worker_id)
        await self.context.ingestion.start(
            include_harvest_scheduler=False,
            resume_historical_rebuilds=False,
        )
        await asyncio.to_thread(self._enqueue_orphaned_rebuilds)
        scheduler_task = asyncio.create_task(self._scheduler_loop(), name="worker-scheduler")
        consumer_task = asyncio.create_task(self._consumer_loop(), name="worker-consumer")
        stop_task = asyncio.create_task(self._stop.wait(), name="worker-stop")
        critical_tasks = (
            scheduler_task,
            consumer_task,
            *self.context.ingestion.critical_runtime_tasks(),
        )
        try:
            done, _ = await asyncio.wait(
                (*critical_tasks, stop_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                if task is stop_task:
                    continue
                exception = task.exception()
                if exception is not None:
                    raise exception
                raise RuntimeError(f"Critical worker task stopped unexpectedly: {task.get_name()}")
        finally:
            for task in critical_tasks:
                task.cancel()
            stop_task.cancel()
            await asyncio.gather(*critical_tasks, stop_task, return_exceptions=True)
            await self.context.ingestion.stop()
            logger.info("worker_stop worker_id=%s", self.worker_id)

    def stop(self) -> None:
        self._stop.set()

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
        for company_slug, cut_at in due_cuts:
            cut_iso = cut_at.isoformat()
            await asyncio.to_thread(
                self.queue.enqueue,
                job_type="harvest_cut",
                payload={"company_slug": company_slug, "cut_at": cut_iso, "force": False},
                priority=PRIORITY_HARVEST_CUT,
                idempotency_key=f"harvest:{company_slug}:{cut_iso}",
                company_slug=company_slug,
            )
        if due_cuts:
            logger.info("worker_harvests_enqueued count=%s", len(due_cuts))

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

    async def _consumer_loop(self) -> None:
        poll_seconds = max(float(self.context.settings.worker_poll_interval_seconds), 0.25)
        while not self._stop.is_set():
            job = await asyncio.to_thread(self.queue.claim, worker_id=self.worker_id)
            if not job:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=poll_seconds)
                except asyncio.TimeoutError:
                    continue
                continue

            heartbeat_task = asyncio.create_task(self._heartbeat_loop(job.id), name=f"heartbeat-{job.id}")
            logger.info(
                "worker_job_claimed job_id=%s job_type=%s company=%s priority=%s attempt=%s",
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
                    worker_id=self.worker_id,
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
                    worker_id=self.worker_id,
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
                    worker_id=self.worker_id,
                    result=result,
                )
                logger.info("worker_job_succeeded job_id=%s job_type=%s", job.id, job.job_type)
            finally:
                heartbeat_task.cancel()
                with suppress(asyncio.CancelledError):
                    await heartbeat_task

    async def _heartbeat_loop(self, job_id: str) -> None:
        interval = max(int(self.context.settings.worker_heartbeat_seconds), 5)
        while True:
            await asyncio.sleep(interval)
            owned = await asyncio.to_thread(
                self.queue.heartbeat,
                job_id=job_id,
                worker_id=self.worker_id,
            )
            if not owned:
                return

    async def _execute(self, job_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.context.registry.reload()
        if job_type == "harvest_cut":
            cut_at = _parse_datetime(payload.get("cut_at"))
            return await self.context.ingestion.run_harvest_cut(
                company_slug=str(payload["company_slug"]),
                cut_at=cut_at,
                force=bool(payload.get("force", False)),
            )
        if job_type == "refresh_snapshot":
            company_slug = str(payload["company_slug"])
            return await self.context.ingestion.run_harvest_cut(
                company_slug=company_slug,
                cut_at=_parse_datetime(payload.get("cut_at")),
                force=True,
            )
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
        if job_type == "purge_mock":
            result = await asyncio.to_thread(self.context.dashboard.purge_mock_legacy)
            self.context.ingestion.mark_dirty()
            return result
        raise ValueError(f"Unsupported background job type: {job_type}")


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
