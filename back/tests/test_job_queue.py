from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.database import Base
from app.core.time import utc_now
from app.models import BackgroundJob
from app.services.job_queue import JobQueue, RetryJob
from app.worker import DashboardWorker


def _queue() -> tuple[JobQueue, sessionmaker]:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=True, future=True)
    settings = SimpleNamespace(
        worker_max_attempts=5,
        worker_lease_seconds=60,
        worker_retry_base_seconds=5,
        worker_retry_max_seconds=60,
    )
    return JobQueue(session_factory=session_factory, settings=settings), session_factory


def test_idempotency_returns_the_same_job() -> None:
    queue, _ = _queue()
    first = queue.enqueue(
        job_type="historical_rebuild",
        payload={"company_slug": "demo"},
        priority=10,
        idempotency_key="rebuild:demo:1",
        company_slug="demo",
    )
    second = queue.enqueue(
        job_type="historical_rebuild",
        payload={"company_slug": "demo", "ignored": True},
        priority=10,
        idempotency_key="rebuild:demo:1",
        company_slug="demo",
    )
    assert first["job_id"] == second["job_id"]


def test_harvest_priority_wins_over_rebuild() -> None:
    queue, _ = _queue()
    queue.enqueue(
        job_type="historical_rebuild",
        payload={},
        priority=10,
        idempotency_key="rebuild:demo:2",
        company_slug="demo",
    )
    harvest = queue.enqueue(
        job_type="harvest_cut",
        payload={},
        priority=100,
        idempotency_key="harvest:demo:cut",
        company_slug="demo",
    )
    claimed = queue.claim(worker_id="worker-a")
    assert claimed is not None
    assert claimed.id == harvest["job_id"]
    assert claimed.job_type == "harvest_cut"
    assert claimed.priority == 100


def test_claim_can_reserve_a_lane_for_harvest_jobs() -> None:
    queue, _ = _queue()
    queue.enqueue(
        job_type="historical_rebuild",
        payload={},
        priority=10,
        idempotency_key="rebuild:demo:lane",
        company_slug="demo",
    )
    harvest = queue.enqueue(
        job_type="harvest_cut",
        payload={},
        priority=100,
        idempotency_key="harvest:demo:lane",
        company_slug="demo",
    )

    claimed = queue.claim(worker_id="harvest-lane", job_types={"harvest_cut", "refresh_snapshot"})

    assert claimed is not None
    assert claimed.id == harvest["job_id"]
    assert queue.has_active_jobs(job_types={"harvest_cut"}) is True


def test_maintenance_lane_waits_while_a_harvest_is_active() -> None:
    queue, _ = _queue()
    queue.enqueue(
        job_type="historical_rebuild",
        payload={},
        priority=10,
        idempotency_key="rebuild:demo:deferred",
        company_slug="demo",
    )
    harvest = queue.enqueue(
        job_type="harvest_cut",
        payload={},
        priority=100,
        idempotency_key="harvest:demo:blocker",
        company_slug="demo",
    )
    claimed_harvest = queue.claim(
        worker_id="harvest-lane",
        job_types={"harvest_cut", "refresh_snapshot"},
    )
    assert claimed_harvest is not None

    deferred = queue.claim(
        worker_id="maintenance-lane",
        job_types={"historical_rebuild"},
        defer_while_job_types_active={"harvest_cut", "refresh_snapshot"},
    )
    assert deferred is None

    queue.complete(job_id=harvest["job_id"], worker_id="harvest-lane", result={"status": "succeeded"})
    claimed_rebuild = queue.claim(
        worker_id="maintenance-lane",
        job_types={"historical_rebuild"},
        defer_while_job_types_active={"harvest_cut", "refresh_snapshot"},
    )
    assert claimed_rebuild is not None
    assert claimed_rebuild.job_type == "historical_rebuild"


def test_expired_lease_is_recovered_by_another_worker() -> None:
    queue, session_factory = _queue()
    created = queue.enqueue(
        job_type="backfill",
        payload={},
        priority=50,
        idempotency_key="backfill:demo:1",
        company_slug="demo",
    )
    claimed = queue.claim(worker_id="worker-a")
    assert claimed is not None
    with session_factory() as session:
        row = session.get(BackgroundJob, created["job_id"])
        assert row is not None
        row.lease_expires_at = utc_now() - timedelta(seconds=1)
        session.add(row)
        session.commit()

    recovered = queue.claim(worker_id="worker-b")
    assert recovered is not None
    assert recovered.id == created["job_id"]
    assert recovered.lease_owner == "worker-b"
    assert recovered.attempts == 2


def test_summary_distinguishes_healthy_and_stale_workers() -> None:
    queue, session_factory = _queue()
    created = queue.enqueue(
        job_type="km_repair",
        payload={},
        priority=70,
        idempotency_key="km-repair:demo:1",
        company_slug="demo",
    )
    claimed = queue.claim(worker_id="worker-a")
    assert claimed is not None

    healthy = queue.summary()
    assert healthy["running"] == 1
    assert healthy["healthy_running"] == 1
    assert healthy["stale_running"] == 0

    with session_factory() as session:
        row = session.get(BackgroundJob, created["job_id"])
        assert row is not None
        row.lease_expires_at = utc_now() - timedelta(seconds=1)
        session.add(row)
        session.commit()

    stale = queue.summary()
    assert stale["running"] == 1
    assert stale["healthy_running"] == 0
    assert stale["stale_running"] == 1


def test_summary_counts_only_failed_jobs_from_the_current_month() -> None:
    queue, session_factory = _queue()
    now = utc_now()
    with session_factory() as session:
        session.add_all(
            [
                BackgroundJob(
                    id="failed-current-month",
                    job_type="harvest_cut",
                    priority=100,
                    payload_json="{}",
                    idempotency_key="failed-current-month",
                    status="failed",
                    attempts=1,
                    max_attempts=1,
                    created_at=now,
                    updated_at=now,
                    finished_at=now,
                ),
                BackgroundJob(
                    id="failed-previous-month",
                    job_type="harvest_cut",
                    priority=100,
                    payload_json="{}",
                    idempotency_key="failed-previous-month",
                    status="failed",
                    attempts=1,
                    max_attempts=1,
                    created_at=now - timedelta(days=40),
                    updated_at=now - timedelta(days=40),
                    finished_at=now - timedelta(days=40),
                ),
            ]
        )
        session.commit()

    assert queue.summary()["failed"] == 1


def test_transient_completed_harvest_is_requeued() -> None:
    queue, _ = _queue()
    created = queue.enqueue(
        job_type="harvest_cut",
        payload={"company_slug": "demo"},
        priority=100,
        idempotency_key="harvest:demo:transient",
        company_slug="demo",
    )
    claimed = queue.claim(worker_id="worker-a")
    assert claimed is not None
    queue.complete(
        job_id=claimed.id,
        worker_id="worker-a",
        result={"status": "maintenance", "error_message": "paused"},
    )

    assert queue.requeue_transient_harvests() == 1
    recovered = queue.get(created["job_id"])
    assert recovered is not None
    assert recovered["status"] == "queued"
    assert recovered["attempts"] == 0
    assert recovered["lease_owner"] is None


def test_successful_harvest_is_not_requeued() -> None:
    queue, _ = _queue()
    created = queue.enqueue(
        job_type="harvest_cut",
        payload={},
        priority=100,
        idempotency_key="harvest:demo:complete",
        company_slug="demo",
    )
    claimed = queue.claim(worker_id="worker-a")
    assert claimed is not None
    queue.complete(
        job_id=claimed.id,
        worker_id="worker-a",
        result={"status": "succeeded"},
    )

    assert queue.requeue_transient_harvests() == 0
    completed = queue.get(created["job_id"])
    assert completed is not None
    assert completed["status"] == "succeeded"


def test_worker_retries_transient_harvest_results() -> None:
    with pytest.raises(RetryJob):
        DashboardWorker._require_completed_harvest(
            {"status": "rate_limited", "error_message": "provider busy"}
        )

    result = {"status": "succeeded", "dms_total": 4}
    assert DashboardWorker._require_completed_harvest(result) == result


def test_latest_harvest_supersedes_older_queued_cut() -> None:
    queue, _ = _queue()
    first_cut = utc_now().replace(second=0, microsecond=0)
    first = queue.enqueue_latest_harvest(
        company_slug="demo",
        cut_at=first_cut,
        payload={"company_slug": "demo", "cut_at": first_cut.isoformat()},
    )
    latest_cut = first_cut + timedelta(minutes=45)
    latest = queue.enqueue_latest_harvest(
        company_slug="demo",
        cut_at=latest_cut,
        payload={"company_slug": "demo", "cut_at": latest_cut.isoformat()},
    )

    assert first["created"] is True
    assert latest["created"] is True
    superseded = queue.get(first["job_id"])
    assert superseded is not None
    assert superseded["status"] == "succeeded"
    assert superseded["result"] == {
        "status": "superseded",
        "superseded_by": latest["job_id"],
    }
    assert queue.summary()["queued"] == 1


def test_latest_harvest_reuses_covering_job() -> None:
    queue, _ = _queue()
    latest_cut = utc_now().replace(second=0, microsecond=0)
    latest = queue.enqueue_latest_harvest(
        company_slug="demo",
        cut_at=latest_cut,
        payload={"company_slug": "demo", "cut_at": latest_cut.isoformat()},
    )
    older = queue.enqueue_latest_harvest(
        company_slug="demo",
        cut_at=latest_cut - timedelta(minutes=15),
        payload={"company_slug": "demo", "cut_at": latest_cut.isoformat()},
    )

    assert older["job_id"] == latest["job_id"]
    assert older["created"] is False
    assert queue.summary()["queued"] == 1


def test_manual_refresh_reuses_covering_harvest() -> None:
    queue, _ = _queue()
    cut_at = utc_now().replace(second=0, microsecond=0)
    harvest = queue.enqueue_latest_harvest(
        company_slug="demo",
        cut_at=cut_at,
        payload={"company_slug": "demo", "cut_at": cut_at.isoformat()},
    )
    refresh = queue.enqueue_latest_refresh(
        company_slug="demo",
        cut_at=cut_at,
        payload={"company_slug": "demo", "cut_at": cut_at.isoformat()},
    )

    assert refresh["job_id"] == harvest["job_id"]
    assert refresh["created"] is False
    assert refresh["reused_for_refresh"] is True


def test_newer_harvest_supersedes_older_queued_refresh() -> None:
    queue, session_factory = _queue()
    old_cut = utc_now().replace(second=0, microsecond=0)
    refresh = queue.enqueue_latest_refresh(
        company_slug="demo",
        cut_at=old_cut,
        payload={"company_slug": "demo", "cut_at": old_cut.isoformat()},
    )
    new_cut = old_cut + timedelta(minutes=15)
    harvest = queue.enqueue_latest_harvest(
        company_slug="demo",
        cut_at=new_cut,
        payload={"company_slug": "demo", "cut_at": new_cut.isoformat()},
    )

    with session_factory() as session:
        stale = session.get(BackgroundJob, refresh["job_id"])
        assert stale is not None
        assert stale.status == "succeeded"
        assert "superseded" in (stale.result_json or "")
    assert harvest["created"] is True


def test_worker_uses_provider_retry_timestamp() -> None:
    retry_at = utc_now() + timedelta(minutes=2)
    with pytest.raises(RetryJob) as exc_info:
        DashboardWorker._require_completed_harvest(
            {
                "status": "rate_limited",
                "error_message": "provider busy",
                "next_retry_at": retry_at.isoformat(),
            }
        )

    assert exc_info.value.retry_at == retry_at


def test_failed_harvest_can_be_requeued_for_the_same_cut() -> None:
    queue, session_factory = _queue()
    cut_at = utc_now().replace(second=0, microsecond=0)
    created = queue.enqueue_latest_harvest(
        company_slug="demo",
        cut_at=cut_at,
        payload={"company_slug": "demo", "cut_at": cut_at.isoformat()},
    )
    with session_factory() as session:
        failed = session.get(BackgroundJob, created["job_id"])
        assert failed is not None
        failed.status = "failed"
        failed.attempts = failed.max_attempts
        failed.last_error = "provider unavailable"
        session.add(failed)
        session.commit()

    recovered = queue.enqueue_latest_harvest(
        company_slug="demo",
        cut_at=cut_at,
        payload={"company_slug": "demo", "cut_at": cut_at.isoformat()},
    )
    assert recovered["job_id"] == created["job_id"]
    assert recovered["status"] == "queued"
    assert recovered["attempts"] == 0
    assert recovered["requeued"] is True
