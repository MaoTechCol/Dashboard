from __future__ import annotations

import socket
from pathlib import Path
from uuid import uuid4

from app.core.systemd import notify_systemd


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
