from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from app.services.report_storage import ReportStorage


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        report_storage_backend="supabase",
        supabase_url="https://project.supabase.co",
        supabase_service_role_key="service-role-key",
        supabase_reports_bucket="dms-reports",
    )


@pytest.mark.parametrize(
    ("status_code", "payload"),
    [
        (404, {}),
        (400, {"statusCode": "404", "code": "NoSuchBucket"}),
    ],
)
def test_ensure_bucket_creates_missing_supabase_bucket(status_code: int, payload: dict[str, str]) -> None:
    missing = Mock(status_code=status_code)
    missing.json.return_value = payload
    created = Mock(status_code=200)

    with patch("app.services.report_storage.httpx.get", return_value=missing), patch(
        "app.services.report_storage.httpx.post", return_value=created
    ) as create_bucket:
        storage = ReportStorage(_settings())
        storage._ensure_supabase_bucket()

    create_bucket.assert_called_once()
    created.raise_for_status.assert_called_once()


def test_ensure_bucket_does_not_create_existing_bucket() -> None:
    existing = Mock(status_code=200)

    with patch("app.services.report_storage.httpx.get", return_value=existing), patch(
        "app.services.report_storage.httpx.post"
    ) as create_bucket:
        storage = ReportStorage(_settings())
        storage._ensure_supabase_bucket()

    create_bucket.assert_not_called()
    existing.raise_for_status.assert_called_once()
