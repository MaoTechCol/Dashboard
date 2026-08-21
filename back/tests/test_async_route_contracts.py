from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.routing import APIRoute

from app.api.routes import _enqueue_snapshot_refresh, router


def _route_status(path: str, method: str) -> int | None:
    for route in router.routes:
        if isinstance(route, APIRoute) and route.path == path and method in route.methods:
            return route.status_code
    raise AssertionError(f"Missing route {method} {path}")


def test_long_running_operations_acknowledge_with_202() -> None:
    assert _route_status("/dashboard/refresh", "POST") == 202
    assert _route_status("/admin/companies", "POST") == 202
    assert _route_status("/admin/companies/{company_slug}/deactivate", "POST") == 202
    assert _route_status("/admin/backfill", "POST") == 202
    assert _route_status("/admin/harvest/rerun-cut", "POST") == 202
    assert _route_status("/admin/harvest/rebuild-history", "POST") == 202
    assert _route_status("/admin/reconciliation/reviews/bulk/approve", "POST") == 202
    assert _route_status("/admin/reconciliation/reviews/bulk/discard", "POST") == 202


def test_snapshot_refresh_reuses_the_official_harvest_job() -> None:
    cut_at = datetime(2026, 8, 21, 3, 0, tzinfo=timezone.utc)

    class FakeJobs:
        def enqueue_latest_harvest(self, **kwargs):
            return {"job_id": "official-cut", **kwargs}

        def enqueue_latest_refresh(self, **kwargs):
            raise AssertionError(f"unexpected duplicate refresh job: {kwargs}")

    context = SimpleNamespace(
        ingestion=SimpleNamespace(latest_due_cut=lambda: cut_at),
        jobs=FakeJobs(),
    )

    result = _enqueue_snapshot_refresh(context, "ismocol")

    assert result["job_id"] == "official-cut"
    assert result["company_slug"] == "ismocol"
    assert result["cut_at"] == cut_at
