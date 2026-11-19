"""WebSocket broadcast hub + persistent event log."""
import asyncio
import json
import logging
from datetime import datetime, timezone

from fastapi import WebSocket

from ..db import db

log = logging.getLogger("events")


def _default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)


class Hub:
    def __init__(self):
        self.clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)

    def disconnect(self, ws: WebSocket):
        self.clients.discard(ws)

    async def broadcast(self, type_: str, data) -> None:
        if not self.clients:
            return
        msg = json.dumps({"type": type_, "data": data, "ts": datetime.now(timezone.utc).isoformat()}, default=_default)
        dead = []
        for ws in list(self.clients):
            try:
                await asyncio.wait_for(ws.send_text(msg), timeout=2)
            except Exception:  # noqa: BLE001
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


hub = Hub()


async def log_event(level: str, source: str, message: str, data: dict | None = None) -> None:
    doc = {"ts": datetime.now(timezone.utc), "level": level, "source": source, "message": message, "data": data or {}}
    getattr(log, level.lower() if level.lower() in ("info", "warning", "error") else "info")("[%s] %s", source, message)
    try:
        await db().event_logs.insert_one(dict(doc))
    except Exception:  # noqa: BLE001
        pass
    await hub.broadcast("log", doc)
