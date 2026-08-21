from __future__ import annotations

import socket
from pathlib import Path
from uuid import uuid4

from app.core.systemd import classify_memory_pressure, notify_systemd, read_process_rss_bytes


def test_notify_systemd_is_optional(monkeypatch) -> None:
    monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
    assert notify_systemd("READY=1") is False


def test_notify_systemd_sends_datagram(monkeypatch) -> None:
    socket_path = Path(f"/tmp/dms-notify-{uuid4().hex[:8]}.sock")
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as server:
            server.bind(str(socket_path))
            server.settimeout(1)
            monkeypatch.setenv("NOTIFY_SOCKET", str(socket_path))

            assert notify_systemd("READY=1") is True
            assert server.recv(1024) == b"READY=1"
    finally:
        socket_path.unlink(missing_ok=True)


def test_read_process_rss_bytes(tmp_path) -> None:
    status_path = tmp_path / "status"
    status_path.write_text("Name:\tpython\nVmRSS:\t   12345 kB\n", encoding="utf-8")

    assert read_process_rss_bytes(status_path) == 12345 * 1024


def test_memory_pressure_thresholds() -> None:
    mib = 1024 * 1024

    assert classify_memory_pressure(449 * mib, warning_mb=450, critical_mb=750) == "normal"
    assert classify_memory_pressure(450 * mib, warning_mb=450, critical_mb=750) == "warning"
    assert classify_memory_pressure(750 * mib, warning_mb=450, critical_mb=750) == "critical"
