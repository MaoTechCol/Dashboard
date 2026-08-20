from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.database import Base
from app.core.time import utc_now
from app.models import BackgroundJob
from app.services.job_queue import JobQueue


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
