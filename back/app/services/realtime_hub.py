from __future__ import annotations

from collections import defaultdict
from typing import Any

from fastapi import WebSocket


class RealtimeHub:
    def __init__(self) -> None:
        self._connections: dict[str, set[WebSocket]] = defaultdict(set)
        self._snapshots: dict[str, dict[str, Any]] = {}

    async def connect(self, company_slug: str, websocket: WebSocket) -> None:
        await websocket.accept()
        self._connections[company_slug].add(websocket)

    def disconnect(self, company_slug: str, websocket: WebSocket) -> None:
        self._connections[company_slug].discard(websocket)

    def snapshot(self, company_slug: str) -> dict[str, Any] | None:
        return self._snapshots.get(company_slug)

    async def publish(self, company_slug: str, payload: dict[str, Any]) -> None:
        self._snapshots[company_slug] = payload
        stale: list[WebSocket] = []
        for websocket in self._connections[company_slug]:
            try:
                await websocket.send_json({"type": "snapshot", "payload": payload})
            except Exception:
                stale.append(websocket)
        for websocket in stale:
            self.disconnect(company_slug, websocket)

    async def keepalive(self, company_slug: str) -> None:
        stale: list[WebSocket] = []
        for websocket in self._connections[company_slug]:
            try:
                await websocket.send_json({"type": "keepalive"})
            except Exception:
                stale.append(websocket)
        for websocket in stale:
            self.disconnect(company_slug, websocket)
