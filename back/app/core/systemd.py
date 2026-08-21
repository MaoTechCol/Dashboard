from __future__ import annotations

import asyncio
import logging
import os
import socket
from pathlib import Path


logger = logging.getLogger("dashboard.resources")


def notify_systemd(message: str) -> bool:
    """Send a systemd notification without an additional runtime dependency."""
    address = os.getenv("NOTIFY_SOCKET")
    if not address:
        return False
    if address.startswith("@"):
        address = "\0" + address[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as client:
            client.connect(address)
            client.sendall(message.encode("utf-8"))
    except OSError:
        return False
    return True


async def watchdog_loop(stop_event: asyncio.Event) -> None:
    watchdog_usec = int(os.getenv("WATCHDOG_USEC", "0") or 0)
    if watchdog_usec <= 0:
        await stop_event.wait()
        return
    interval = max(watchdog_usec / 1_000_000 / 3, 1.0)
    while not stop_event.is_set():
        notify_systemd("WATCHDOG=1")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue


def read_process_rss_bytes(status_path: Path = Path("/proc/self/status")) -> int:
    """Read current RSS on Linux without adding psutil to production."""
    try:
        for line in status_path.read_text(encoding="utf-8").splitlines():
            if not line.startswith("VmRSS:"):
                continue
            parts = line.split()
            return int(parts[1]) * 1024
    except (OSError, ValueError, IndexError):
        return 0
    return 0


def classify_memory_pressure(rss_bytes: int, *, warning_mb: int, critical_mb: int) -> str:
    rss_mb = rss_bytes / (1024 * 1024)
    if rss_mb >= critical_mb:
        return "critical"
    if rss_mb >= warning_mb:
        return "warning"
    return "normal"


async def memory_monitor_loop(
    stop_event: asyncio.Event,
    *,
    role: str,
    warning_mb: int,
    critical_mb: int,
    interval_seconds: int,
) -> None:
    """Emit actionable journal/systemd signals only when pressure changes."""
    previous_level = "normal"
    interval = max(int(interval_seconds), 5)
    while not stop_event.is_set():
        rss_bytes = read_process_rss_bytes()
        level = classify_memory_pressure(
            rss_bytes,
            warning_mb=max(int(warning_mb), 1),
            critical_mb=max(int(critical_mb), int(warning_mb) + 1),
        )
        rss_mb = rss_bytes / (1024 * 1024)
        if level != previous_level:
            if level == "critical":
                logger.error(
                    "process_memory_critical role=%s rss_mb=%.1f warning_mb=%s critical_mb=%s",
                    role,
                    rss_mb,
                    warning_mb,
                    critical_mb,
                )
                notify_systemd(f"STATUS={role} memoria critica: {rss_mb:.0f} MiB")
            elif level == "warning":
                logger.warning(
                    "process_memory_warning role=%s rss_mb=%.1f warning_mb=%s critical_mb=%s",
                    role,
                    rss_mb,
                    warning_mb,
                    critical_mb,
                )
                notify_systemd(f"STATUS={role} memoria alta: {rss_mb:.0f} MiB")
            else:
                logger.info("process_memory_recovered role=%s rss_mb=%.1f", role, rss_mb)
                notify_systemd(f"STATUS={role} disponible; memoria {rss_mb:.0f} MiB")
            previous_level = level
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue
