from __future__ import annotations

import asyncio
import os
import socket


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
