from __future__ import annotations

from types import SimpleNamespace

from app.services.howen import HowenClient


def _client() -> HowenClient:
    settings = SimpleNamespace(
        howen_http_base="https://provider.example/vss",
        howen_username="shared-account",
        howen_request_spacing_seconds=2.5,
        howen_request_spacing_max_seconds=8.0,
        howen_request_recovery_successes=2,
        backfill_rate_limit_cooldown_seconds=20,
    )
    client = HowenClient(settings=settings, registry=SimpleNamespace())
    HowenClient._adaptive_request_spacing.pop(client._account_key, None)
    HowenClient._successful_request_streak.pop(client._account_key, None)
    HowenClient._next_request_at.pop(client._account_key, None)
    return client


def test_rate_limit_increases_spacing_and_successes_recover_gradually() -> None:
    client = _client()

    client._register_rate_limit()
    limited_spacing = client._current_request_spacing()
    assert limited_spacing == 3.75

    client._register_request_success()
    client._register_request_success()
    assert client._current_request_spacing() == 3.25

    client._register_request_success()
    client._register_request_success()
    assert client._current_request_spacing() == 2.75


def test_clients_for_the_same_account_share_the_provider_lane() -> None:
    first = _client()
    second = HowenClient(settings=first.settings, registry=SimpleNamespace())

    assert first._get_request_lock() is second._get_request_lock()
