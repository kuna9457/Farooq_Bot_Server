"""FastAPI entry point:  uvicorn app.main:app --reload --port 8000"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from . import db as dbmod
from .api import auth, routes
from .config import get_settings
from .core.events import hub, log_event
from .core.scheduler import scheduler, start_scheduler
from .data.universe import refresh_fundamentals, refresh_universe
from .strategy.engine import engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
log = logging.getLogger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    s = get_settings()
    await dbmod.connect()
    await refresh_universe()
    import asyncio
    asyncio.create_task(refresh_fundamentals())
    await log_event("info", "main", f"Backend started (data={s.data_provider}, mock_db={dbmod.using_mock})")
    if s.enable_scheduler:
        asyncio.create_task(start_scheduler())
        engine.start_ltp_loop(s.ltp_poll_seconds)
    yield
    if scheduler.running:
        scheduler.shutdown(wait=False)


app = FastAPI(title="WelthWest Algo", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=get_settings().cors_list, allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])
app.include_router(auth.router)
app.include_router(routes.api)
app.include_router(routes.public)


@app.get("/health")
async def health():
    return {"ok": True, "mock_db": dbmod.using_mock}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket, token: str = ""):
    try:
        auth.verify_token(token)
    except Exception:  # noqa: BLE001
        await ws.close(code=4401)
        return
    await hub.connect(ws)
    try:
        while True:
            await ws.receive_text()  # keepalive pings from the client
    except WebSocketDisconnect:
        hub.disconnect(ws)
    except Exception:  # noqa: BLE001
        hub.disconnect(ws)
