from __future__ import annotations

import asyncio

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


@router.websocket("/ws/dashboard/{company_slug}")
async def dashboard_socket(company_slug: str, websocket: WebSocket) -> None:
    context = websocket.app.state.context
    if company_slug not in {company.slug for company in context.registry.all()}:
        await websocket.close(code=4404)
        return

    try:
        await context.hub.connect(company_slug, websocket)
        snapshot = context.hub.snapshot(company_slug) or context.dashboard.build_snapshot(company_slug)
        await websocket.send_json({"type": "snapshot", "payload": snapshot})
        while True:
            await asyncio.sleep(25)
            await context.hub.keepalive(company_slug)
    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    except Exception:
        pass
    finally:
        context.hub.disconnect(company_slug, websocket)
