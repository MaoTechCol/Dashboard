from __future__ import annotations

from fastapi.routing import APIRoute

from app.api.routes import router


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
