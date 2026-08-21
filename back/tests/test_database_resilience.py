from __future__ import annotations

from app.core.database import is_database_timeout
from app.core.settings import Settings


def test_database_timeout_recognizes_nested_driver_error() -> None:
    try:
        try:
            raise RuntimeError("canceling statement due to statement timeout")
        except RuntimeError as exc:
            raise ValueError("database request failed") from exc
    except ValueError as exc:
        assert is_database_timeout(exc) is True


def test_database_timeout_does_not_hide_regular_errors() -> None:
    assert is_database_timeout(RuntimeError("duplicate key value violates unique constraint")) is False


def test_role_specific_resource_limits() -> None:
    api_settings = Settings(process_role="api", _env_file=None)
    worker_settings = Settings(process_role="worker", _env_file=None)

    assert api_settings.database_statement_timeout_ms == 12_000
    assert api_settings.database_pool_timeout_seconds == 5
    assert api_settings.memory_critical_mb == 750
    assert worker_settings.database_statement_timeout_ms == 300_000
    assert worker_settings.database_pool_timeout_seconds == 30
    assert worker_settings.memory_critical_mb == 2_000
