"""REST API."""
import asyncio
from datetime import date, datetime, timedelta, timezone

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from ..backtest.engine import run_backtest
from ..config import get_settings
from ..core.events import log_event
from ..core.scheduler import jobs_info, reschedule_from_params
from ..core.state import get_params, get_runtime, save_params, update_runtime
from ..data.market_data import market_data, now_ist
from ..db import clean, db
from ..execution.brokers import BrokerError, PaperBroker, get_broker
from ..execution.order_manager import order_manager, today_str
from ..execution.tokens import clear_token, save_token, token_status
from ..strategy.engine import engine, market_open_now
from ..strategy.params import StrategyParams
from .auth import current_user
from .charts import build_chart

api = APIRouter(prefix="/api", dependencies=[Depends(current_user)])
public = APIRouter(prefix="/api")   # broker OAuth callbacks (browser redirects, no bearer header)


# ============================================================================ status / session
@api.get("/status")
async def status():
    from .. import db as dbmod
    rt = await get_runtime()
    return {"runtime": rt, "mock_db": dbmod.using_mock, "data_provider": market_data.provider.name,
            "market_open": market_open_now(), "now_ist": now_ist().isoformat(), "engine": engine.status,
            "jobs": jobs_info(),
            "brokers": {b: await token_status(b) for b in ("angel", "upstox", "kite")}}


@api.get("/session")
async def session(date_: str | None = Query(None, alias="date")):
    d = date_ or today_str()
    s = await db().daily_sessions.find_one({"date": d})
    filters = [clean(x) async for x in db().filter_states.find({"date": d})]
    return {"date": d, "session": clean(s), "filters": filters, "ltp": engine.ltps}


@api.get("/sessions")
async def sessions(limit: int = 30):
    cur = db().daily_sessions.find({}, {"buy_candidates": 1, "sell_candidates": 1, "date": 1, "regime": 1,
                                        "nifty_change": 1, "eod_pnl": 1}).sort("date", -1).limit(limit)
    return [clean(x) async for x in cur]


@api.get("/filter-history")
async def filter_history(symbol: str, date_: str | None = Query(None, alias="date")):
    d = date_ or today_str()
    return [clean(x) async for x in db().filter_history.find({"date": d, "symbol": symbol}).sort("ts", 1)]


# ============================================================================ engine controls
@api.post("/engine/scan")
async def run_scan():
    return clean(await engine.pre_market_scan())


@api.post("/engine/regime")
async def run_regime():
    return clean(await engine.decide_regime())


@api.post("/engine/evaluate")
async def run_evaluate(include_inactive: bool = True):
    # manual evaluation never places orders unless the market is open AND engine is enabled
    return clean(await engine.evaluate_all(allow_orders=market_open_now(), include_inactive=include_inactive))


@api.post("/engine/square-off")
async def square_off():
    return {"closed": await engine.square_off("Manual square-off")}


@api.post("/engine/kill-switch")
async def kill_switch():
    await update_runtime(halted_date=today_str(), engine_enabled=False)
    n = await engine.square_off("KILL SWITCH")
    await log_event("warning", "engine", f"KILL SWITCH: closed {n}, auto-trading disabled")
    return {"closed": n}


@api.post("/engine/resume")
async def resume():
    await update_runtime(halted_date=None, engine_enabled=True)
    await log_event("info", "engine", "Auto-trading resumed")
    return await get_runtime()


# ============================================================================ chart
@api.get("/chart/{symbol}")
async def chart(symbol: str, tf: str = "5m", side: str = "BUY"):
    p = await get_params()
    try:
        return await build_chart(symbol, tf, side, p)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(400, f"{type(e).__name__}: {e}") from e


# ============================================================================ account / trading
@api.get("/account/summary")
async def account_summary():
    rt = await get_runtime()
    out = {"mode": rt["mode"], "execution_broker": rt["broker"] if rt["mode"] == "live" else "paper",
           "paper": await PaperBroker().funds(), "brokers": {}, "day_pnl": await order_manager.day_pnl()}
    for b in ("angel", "upstox", "kite"):
        st = await token_status(b)
        if st["connected"]:
            try:
                f = await get_broker(b).funds()
                f.pop("raw", None)
                out["brokers"][b] = {"connected": True, **f}
            except Exception as e:  # noqa: BLE001
                out["brokers"][b] = {"connected": True, "error": str(e)}
        else:
            out["brokers"][b] = {"connected": False}
    # all-time stats for the current mode
    tot = n = w = 0
    async for t in db().trades.find({"mode": rt["mode"]}, {"net_pnl": 1}):
        tot += t["net_pnl"]
        n += 1
        w += t["net_pnl"] > 0
    out["all_time"] = {"net_pnl": round(tot, 2), "trades": n, "win_rate": round(100 * w / n, 1) if n else None}
    return out


@api.get("/positions")
async def positions(status: str = "OPEN"):
    q = {} if status == "ALL" else {"status": status}
    if status != "OPEN":
        q["date"] = today_str()
    return [clean(p) async for p in db().positions.find(q).sort("entry_time", -1)]


@api.post("/positions/{pid}/exit")
async def exit_position(pid: str):
    pos = await db().positions.find_one({"_id": ObjectId(pid), "status": "OPEN"})
    if not pos:
        raise HTTPException(404, "Open position not found")
    ltp = (await market_data.ltp([pos["symbol"]])).get(pos["symbol"]) or pos.get("ltp") or pos["entry_price"]
    t = await order_manager.exit(pos, ltp, "Manual exit")
    if not t:
        raise HTTPException(500, "Exit failed — check logs")
    return clean(t)


@api.get("/broker/{name}/positions")
async def broker_positions(name: str):
    try:
        return await get_broker(name).positions()
    except BrokerError as e:
        raise HTTPException(400, str(e)) from e


@api.get("/trades")
async def trades(from_: str | None = Query(None, alias="from"), to: str | None = None, mode: str | None = None,
                 symbol: str | None = None, limit: int = 500):
    q: dict = {}
    if from_ or to:
        q["date"] = {k: v for k, v in (("$gte", from_), ("$lte", to)) if v}
    if mode:
        q["mode"] = mode
    if symbol:
        q["symbol"] = symbol.upper()
    return [clean(t) async for t in db().trades.find(q).sort("exit_time", -1).limit(limit)]


@api.get("/orders")
async def orders(date_: str | None = Query(None, alias="date")):
    return [clean(o) async for o in db().orders.find({"date": date_ or today_str()}).sort("ts", -1)]


@api.get("/pnl/intraday")
async def pnl_intraday(date_: str | None = Query(None, alias="date")):
    return [clean(x) async for x in db().pnl_snapshots.find({"date": date_ or today_str()}).sort("ts", 1)]


@api.get("/pnl/daily")
async def pnl_daily(mode: str | None = None, days: int = 90):
    rt = await get_runtime()
    since = (date.today() - timedelta(days=days)).isoformat()
    agg = {}
    async for t in db().trades.find({"mode": mode or rt["mode"], "date": {"$gte": since}}):
        a = agg.setdefault(t["date"], {"date": t["date"], "net_pnl": 0.0, "trades": 0})
        a["net_pnl"] = round(a["net_pnl"] + t["net_pnl"], 2)
        a["trades"] += 1
    return sorted(agg.values(), key=lambda x: x["date"])


@api.get("/logs")
async def logs(limit: int = 200):
    return [clean(x) async for x in db().event_logs.find({}).sort("ts", -1).limit(limit)]


# ============================================================================ settings
@api.get("/settings/strategy")
async def get_strategy():
    return (await get_params()).model_dump()


@api.put("/settings/strategy")
async def put_strategy(p: StrategyParams):
    v = await save_params(p)
    await reschedule_from_params()
    engine._prep = {}
    await log_event("info", "settings", f"Strategy params saved (v{v})")
    return {"version": v, "params": p.model_dump()}


@api.post("/settings/strategy/reset")
async def reset_strategy():
    v = await save_params(StrategyParams(), "reset to defaults")
    return {"version": v}


class RuntimeIn(BaseModel):
    mode: str | None = None
    broker: str | None = None
    engine_enabled: bool | None = None
    paper_capital: float | None = None
    confirm_live: bool = False


@api.get("/settings/runtime")
async def get_rt():
    return await get_runtime()


@api.put("/settings/runtime")
async def put_rt(body: RuntimeIn):
    ch = {k: v for k, v in body.model_dump().items() if v is not None and k != "confirm_live"}
    if ch.get("mode") == "live":
        if not body.confirm_live:
            raise HTTPException(400, "Switching to LIVE requires confirm_live=true")
        broker = ch.get("broker") or (await get_runtime())["broker"]
        if not (await token_status(broker))["connected"]:
            raise HTTPException(400, f"{broker} is not connected — login first")
    if (await order_manager.open_positions()) and "mode" in ch:
        raise HTTPException(400, "Close open positions before switching mode")
    rt = await update_runtime(**ch)
    await log_event("warning" if ch.get("mode") == "live" else "info", "settings", f"Runtime updated: {ch}")
    return rt


# ============================================================================ brokers
@api.get("/broker/status")
async def broker_status():
    s = get_settings()
    keys = {"angel": s.angel_api_key, "upstox": s.upstox_api_key, "kite": s.kite_api_key}
    out = {b: {**await token_status(b), "api_key_configured": bool(keys[b])} for b in ("angel", "upstox", "kite")}
    from ..execution import angel as _angel
    c = await _angel.creds()
    out["angel"]["auto_login"] = bool(s.angel_api_key and c)
    out["angel"]["saved_client_code"] = c["client_code"] if c else None
    out["angel"]["creds_source"] = c["source"] if c else None
    out["angel"]["websocket"] = _angel.feed_ws.connected
    return out


@api.get("/broker/{name}/login-url")
async def login_url(name: str):
    s = get_settings()
    if name == "angel":
        if not s.angel_api_key:
            raise HTTPException(400, "ANGEL_API_KEY not set")
        return {"url": f"https://smartapi.angelone.in/publisher-login?api_key={s.angel_api_key}"}
    if name == "upstox":
        if not s.upstox_api_key:
            raise HTTPException(400, "UPSTOX_API_KEY not set in backend/.env")
        return {"url": f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code"
                       f"&client_id={s.upstox_api_key}&redirect_uri={s.upstox_redirect_uri}"}
    if name == "kite":
        if not s.kite_api_key:
            raise HTTPException(400, "KITE_API_KEY not set in backend/.env")
        return {"url": f"https://kite.zerodha.com/connect/login?v=3&api_key={s.kite_api_key}"}
    raise HTTPException(404)


class TokenIn(BaseModel):
    access_token: str


@api.post("/broker/{name}/token")
async def set_token(name: str, body: TokenIn):
    if name not in ("angel", "upstox", "kite"):
        raise HTTPException(404)
    if name == "angel":
        from ..execution import angel as _angel
        await _angel.store_session(body.access_token.strip(), "", "", get_settings().angel_client_code)
    else:
        await save_token(name, body.access_token.strip())
    return await token_status(name)


class AngelLoginIn(BaseModel):
    client_code: str
    pin: str
    totp: str = ""            # current 6-digit code from the authenticator app (one-time login) ...
    totp_secret: str = ""     # ... or the TOTP secret (base32) -> enables automatic daily login
    remember: bool = True     # store PIN + TOTP secret (encrypted) so the bot can log in by itself each day


@api.post("/broker/angel/login")
async def angel_login(body: AngelLoginIn):
    """The user enters their own Angel One client code, PIN and TOTP here; nothing is read from server config."""
    from ..execution import angel as _angel
    cc, pin = body.client_code.strip().upper(), body.pin.strip()
    secret, code = body.totp_secret.replace(" ", "").strip(), body.totp.strip()
    if not cc or not pin:
        raise HTTPException(400, "Client code and PIN are required")
    if secret:
        try:
            code = _angel.totp_now(secret)
        except Exception:  # noqa: BLE001
            raise HTTPException(400, "TOTP secret is not valid (expected the base32 text shown next to the QR code)") from None
    if not code:
        raise HTTPException(400, "Enter the 6-digit TOTP code or your TOTP secret")
    try:
        await _angel.login_totp(cc, pin, code)
    except _angel.AngelError as e:
        raise HTTPException(400, str(e)) from e
    if secret and body.remember:
        await _angel.save_creds(cc, pin, secret)
    await log_event("info", "broker", f"Angel One connected ({cc}, {'auto-login saved' if secret and body.remember else 'one-time'})")
    return await token_status("angel")


@api.post("/broker/angel/auto-login")
async def angel_auto_login():
    """Log in again with the credentials the user saved earlier (no typing)."""
    from ..execution import angel as _angel
    try:
        await _angel.login_totp()
    except _angel.AngelError as e:
        raise HTTPException(400, str(e)) from e
    await log_event("info", "broker", "Angel One connected (saved credentials)")
    return await token_status("angel")


@api.post("/broker/angel/forget")
async def angel_forget():
    from ..execution import angel as _angel
    await _angel.forget_creds()
    await clear_token("angel")
    return await token_status("angel")


@api.post("/broker/{name}/logout")
async def broker_logout(name: str):
    await clear_token(name)
    return await token_status(name)


@api.get("/broker/{name}/profile")
async def broker_profile(name: str):
    try:
        return await get_broker(name).profile()
    except BrokerError as e:
        raise HTTPException(400, str(e)) from e


@public.get("/broker/angel/callback")
async def angel_callback(auth_token: str = "", feed_token: str = "", refresh_token: str = ""):
    """Redirect URL for the SmartAPI app (Publisher login): Angel appends auth_token, feed_token, refresh_token."""
    from ..execution import angel as _angel
    s = get_settings()
    if not auth_token:
        return RedirectResponse(f"{s.frontend_url}/broker?error=Angel One login was not completed")
    await _angel.store_session(auth_token, feed_token, refresh_token, s.angel_client_code)
    await log_event("info", "broker", "Angel One connected (publisher login)")
    return RedirectResponse(f"{s.frontend_url}/broker?connected=angel")


@public.get("/broker/upstox/callback")
async def upstox_callback(code: str):
    import httpx
    s = get_settings()
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post("https://api.upstox.com/v2/login/authorization/token",
                         data={"code": code, "client_id": s.upstox_api_key, "client_secret": s.upstox_api_secret,
                               "redirect_uri": s.upstox_redirect_uri, "grant_type": "authorization_code"},
                         headers={"Accept": "application/json"})
    js = r.json()
    if r.status_code >= 400 or "access_token" not in js:
        return RedirectResponse(f"{s.frontend_url}/broker?error={str(js)[:200]}")
    await save_token("upstox", js["access_token"], {"user_name": js.get("user_name"), "user_id": js.get("user_id")})
    await log_event("info", "broker", f"Upstox connected ({js.get('user_name')})")
    return RedirectResponse(f"{s.frontend_url}/broker?connected=upstox")


@public.get("/broker/kite/callback")
async def kite_callback(request_token: str, status: str = "success"):
    from kiteconnect import KiteConnect
    s = get_settings()
    try:
        k = KiteConnect(api_key=s.kite_api_key)
        data = await asyncio.to_thread(k.generate_session, request_token, api_secret=s.kite_api_secret)
    except Exception as e:  # noqa: BLE001
        return RedirectResponse(f"{s.frontend_url}/broker?error={str(e)[:200]}")
    await save_token("kite", data["access_token"], {"user_name": data.get("user_name"), "user_id": data.get("user_id")})
    await log_event("info", "broker", f"Kite connected ({data.get('user_name')})")
    return RedirectResponse(f"{s.frontend_url}/broker?connected=kite")


# ============================================================================ backtest
class BacktestIn(BaseModel):
    start: date
    end: date
    capital: float = 1_000_000
    params: StrategyParams | None = None
    name: str = ""


@api.post("/backtest/run")
async def backtest_run(body: BacktestIn):
    if body.end < body.start:
        raise HTTPException(400, "end < start")
    p = body.params or await get_params()
    doc = {"name": body.name or f"{body.start}→{body.end}", "start": body.start.isoformat(), "end": body.end.isoformat(),
           "capital": body.capital, "params": p.model_dump(), "status": "QUEUED", "progress": 0,
           "data_provider": market_data.provider.name, "created_at": datetime.now(timezone.utc)}
    ins = await db().backtest_runs.insert_one(doc)
    rid = str(ins.inserted_id)
    asyncio.create_task(run_backtest(rid, p, body.start, body.end, body.capital))
    return {"run_id": rid}


@api.get("/backtest")
async def backtest_list():
    return [clean(x) async for x in db().backtest_runs.find({}, {"days": 0, "params": 0, "traceback": 0,
                                                                 "metrics.equity_curve": 0, "metrics.by_symbol": 0,
                                                                 "metrics.by_exit_reason": 0}).sort("created_at", -1).limit(50)]


@api.get("/backtest/{rid}")
async def backtest_get(rid: str):
    d = await db().backtest_runs.find_one({"_id": ObjectId(rid)})
    if not d:
        raise HTTPException(404)
    return clean(d)


@api.get("/backtest/{rid}/trades")
async def backtest_trades(rid: str):
    return [clean(t) async for t in db().backtest_trades.find({"run_id": rid}).sort("entry_time", 1)]


@api.delete("/backtest/{rid}")
async def backtest_delete(rid: str):
    await db().backtest_runs.delete_one({"_id": ObjectId(rid)})
    await db().backtest_trades.delete_many({"run_id": rid})
    return {"ok": True}
