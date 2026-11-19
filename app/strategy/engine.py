"""Live strategy engine: 08:30 scan -> 09:30 regime -> 5-min evaluations -> exits -> 15:15 square-off."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timezone

import pandas as pd

from ..core.events import hub, log_event
from ..core.state import get_params, get_runtime, update_runtime
from ..data.market_data import completed_5m, market_data, now_ist
from ..data.universe import NIFTY_INDEX, get_universe, refresh_fundamentals
from ..db import clean, db
from ..execution.order_manager import order_manager, today_str
from .evaluator import evaluate, prepare_day
from .filters import decide_regime, exit_signal_5m, month_extreme_flags, pick_candidates

log = logging.getLogger("engine")


def _t(s: str) -> time:
    return datetime.strptime(s, "%H:%M").time()


def market_open_now() -> bool:
    n = now_ist()
    return n.weekday() < 5 and time(9, 15) <= n.time() <= time(15, 30)


class Engine:
    def __init__(self):
        self.ltps: dict[str, float] = {}
        self._prep: dict[str, dict] = {}          # symbol -> prepare_day result (today)
        self._prep_date: str | None = None
        self._busy = asyncio.Lock()
        self._ltp_task: asyncio.Task | None = None
        self.status = {"last_scan": None, "last_regime": None, "last_eval": None, "last_ltp": None}

    # ------------------------------------------------------------------------------ session doc
    async def session(self, date: str | None = None) -> dict | None:
        return await db().daily_sessions.find_one({"date": date or today_str()})

    async def _save_session(self, **fields) -> dict:
        d = today_str()
        await db().daily_sessions.update_one({"date": d}, {"$set": {"date": d, **fields}}, upsert=True)
        s = await self.session(d)
        await hub.broadcast("session", clean(s))
        return s

    # ------------------------------------------------------------------------------ 08:30 scan
    async def pre_market_scan(self) -> dict:
        async with self._busy:
            p = await get_params()
            await log_event("info", "scan", "Pre-market scan started")
            universe = await get_universe()
            shares = await refresh_fundamentals()
            sem = asyncio.Semaphore(6)

            async def one(u):
                async with sem:
                    try:
                        d = await market_data.completed_daily(u["symbol"])
                        flags = month_extreme_flags(d, p.scan)
                        sh = shares.get(u["symbol"])
                        mcap = sh * flags["last_close"] if sh and flags.get("last_close") else None
                        return {"symbol": u["symbol"], "name": u.get("name"), "industry": u.get("industry"),
                                "market_cap": mcap, "market_cap_cr": round(mcap / 1e7) if mcap else None, **flags}
                    except Exception as e:  # noqa: BLE001
                        log.warning("scan %s failed: %s", u["symbol"], e)
                        return None
            rows = [r for r in await asyncio.gather(*[one(u) for u in universe]) if r]
            buys, sells = pick_candidates(rows, p.scan.top_n)
            for i, r in enumerate(buys):
                r.update(rank=i + 1, side="BUY")
            for i, r in enumerate(sells):
                r.update(rank=i + 1, side="SELL")
            s = await self._save_session(buy_candidates=buys, sell_candidates=sells, scanned_at=datetime.now(timezone.utc),
                                         scan_universe_size=len(rows),
                                         month_high_count=sum(r["month_high"] for r in rows),
                                         month_low_count=sum(r["month_low"] for r in rows))
            self._prep, self._prep_date = {}, None
            self.status["last_scan"] = datetime.now(timezone.utc)
            await log_event("info", "scan", f"Scan done: BUY {[r['symbol'] for r in buys]} | SELL {[r['symbol'] for r in sells]}")
            return s

    # ------------------------------------------------------------------------------ 09:30 regime
    async def decide_regime(self) -> dict:
        p = await get_params()
        s = await self.session() or await self.pre_market_scan()
        daily = await market_data.completed_daily(NIFTY_INDEX, force=True)
        prev_close = float(daily["close"].iloc[-1])
        bars = completed_5m(await market_data.intraday_5m(NIFTY_INDEX, days=3, ttl=0))
        today = bars[bars.index.date == now_ist().date()]
        cutoff = pd.Timestamp.combine(now_ist().date(), _t(p.regime.decision_time)).tz_localize("Asia/Kolkata")
        upto = today[today.index + pd.Timedelta(minutes=5) <= cutoff]
        if not upto.empty:
            price, src = float(upto["close"].iloc[-1]), f"5m close {upto.index[-1].strftime('%H:%M')}"
        else:
            ltp = (await market_data.ltp([NIFTY_INDEX])).get(NIFTY_INDEX)
            if ltp is None:
                await log_event("warning", "regime", "Nifty price unavailable — regime not decided")
                return s
            price, src = ltp, "LTP"
        change = round(price - prev_close, 2)
        regime = decide_regime(change, p.regime)
        if regime in ("BUY", "SELL") and regime not in p.sides_enabled:
            regime = "NO_TRADE"
        s = await self._save_session(nifty_prev_close=prev_close, nifty_price=price, nifty_change=change,
                                     nifty_price_source=src, regime=regime, regime_rule=p.regime.rule,
                                     regime_threshold=p.regime.threshold_points,
                                     regime_at=datetime.now(timezone.utc))
        self.status["last_regime"] = datetime.now(timezone.utc)
        await log_event("info", "regime", f"Nifty {price:.2f} vs prev {prev_close:.2f} = {change:+.2f} pts -> {regime} day")
        await hub.broadcast("regime_decided", {"regime": regime, "change": change})
        return s

    # ------------------------------------------------------------------------------ candidates
    async def active_candidates(self, s: dict | None = None) -> list[dict]:
        s = s or await self.session()
        if not s:
            return []
        regime = s.get("regime")
        if regime == "BUY":
            return s.get("buy_candidates", [])
        if regime == "SELL":
            return s.get("sell_candidates", [])
        return []

    async def _prepare(self, symbol: str, side: str, p) -> dict:
        d = today_str()
        if self._prep_date != d:
            self._prep, self._prep_date = {}, d
        key = f"{symbol}|{side}"
        if key not in self._prep:
            daily = await market_data.completed_daily(symbol)
            bars = completed_5m(await market_data.intraday_5m(symbol, days=10))
            before = bars[bars.index.date < now_ist().date()]
            self._prep[key] = prepare_day(daily, before, side, p)
        return self._prep[key]

    # ------------------------------------------------------------------------------ evaluation
    async def evaluate_all(self, allow_orders: bool = True, include_inactive: bool = False) -> list[dict]:
        """Evaluate filters for today's active candidates (or all 6 with include_inactive)."""
        async with self._busy:
            p = await get_params()
            s = await self.session()
            if not s:
                return []
            cands = (s.get("buy_candidates", []) + s.get("sell_candidates", [])) if include_inactive \
                else await self.active_candidates(s)
            results = []
            for c in cands:
                sym, side = c["symbol"], c["side"]
                prep: dict = {}
                try:
                    prep = await self._prepare(sym, side, p)
                    daily = await market_data.completed_daily(sym)
                    bars = completed_5m(await market_data.intraday_5m(sym, days=10))
                    res = evaluate(daily, bars, now_ist().date(), side, p, prep)
                except Exception as e:  # noqa: BLE001
                    log.exception("evaluate %s", sym)
                    res = {"F1": {"status": "ERROR", "reason": str(e)}, "F2": {"status": "ERROR", "reason": str(e)},
                           "F3": {"status": "ERROR", "reason": str(e)}, "all_passed": False, "price": None}
                active = side == s.get("regime")
                doc = {"date": today_str(), "symbol": sym, "side": side, "active": active, **res,
                       "setup": prep.get("setup"), "poc": prep.get("poc"),
                       "evaluated_at": datetime.now(timezone.utc)}
                await db().filter_states.update_one({"date": doc["date"], "symbol": sym}, {"$set": doc}, upsert=True)
                await db().filter_history.insert_one({k: doc[k] for k in ("date", "symbol", "side", "all_passed", "price")}
                                                     | {"ts": doc["evaluated_at"],
                                                        "F1": res["F1"]["status"], "F2": res["F2"]["status"],
                                                        "F3": res["F3"]["status"]})
                await hub.broadcast("filter_update", clean(doc))
                results.append(doc)

                if (allow_orders and active and res["all_passed"] and market_open_now()
                        and now_ist().time() < _t(p.risk.last_entry_time)):
                    price = self.ltps.get(sym) or res["price"]
                    await order_manager.enter(sym, side, price, context=res)
            await self._check_exits(p)
            self.status["last_eval"] = datetime.now(timezone.utc)
            return results

    async def _check_exits(self, p) -> None:
        for pos in await order_manager.open_positions():
            try:
                bars = completed_5m(await market_data.intraday_5m(pos["symbol"], days=10))
                flip, info = exit_signal_5m(bars.iloc[-300:], pos["side"], p.filter3)
                et = pd.Timestamp(pos["entry_time"])
                entry_ts = (et if et.tzinfo else et.tz_localize("UTC")).tz_convert("Asia/Kolkata")
                if flip and not bars.empty and bars.index[-1] + pd.Timedelta(minutes=5) > entry_ts:
                    price = self.ltps.get(pos["symbol"]) or float(bars["close"].iloc[-1])
                    await order_manager.exit(pos, price, f"Chandelier 5m {'SELL' if pos['side'] == 'BUY' else 'BUY'} signal")
            except Exception as e:  # noqa: BLE001
                log.warning("exit check %s failed: %s", pos["symbol"], e)

    # ------------------------------------------------------------------------------ LTP loop
    async def ltp_tick(self) -> None:
        s = await self.session()
        syms = {NIFTY_INDEX}
        if s:
            syms |= {c["symbol"] for c in s.get("buy_candidates", []) + s.get("sell_candidates", [])}
        syms |= {p["symbol"] for p in await order_manager.open_positions()}
        ltps = await market_data.ltp(sorted(syms))
        if not ltps:
            return
        self.ltps.update(ltps)
        self.status["last_ltp"] = datetime.now(timezone.utc)
        positions = await order_manager.mark_to_market(ltps)
        p = await get_params()
        # software stop-loss / target
        for pos in positions:
            ltp = ltps.get(pos["symbol"])
            if ltp is None:
                continue
            long = pos["side"] == "BUY"
            if pos.get("sl") and ((long and ltp <= pos["sl"]) or (not long and ltp >= pos["sl"])):
                await order_manager.exit(pos, ltp, f"Stop-loss {pos['sl']}")
            elif pos.get("target") and ((long and ltp >= pos["target"]) or (not long and ltp <= pos["target"])):
                await order_manager.exit(pos, ltp, f"Target {pos['target']}")
        pnl = await order_manager.day_pnl()
        await hub.broadcast("tick", {"ltp": ltps, "pnl": pnl, "positions": clean(await order_manager.open_positions())})
        # daily loss limit
        if p.risk.daily_loss_limit > 0 and pnl["total"] <= -p.risk.daily_loss_limit:
            rt = await get_runtime()
            if rt.get("halted_date") != today_str():
                await update_runtime(halted_date=today_str())
                await log_event("warning", "risk", f"Daily loss limit hit ({pnl['total']}). Squaring off & halting.")
                await order_manager.square_off_all("Daily loss limit", self.ltps)

    async def snapshot_pnl(self) -> None:
        pnl = await order_manager.day_pnl()
        await db().pnl_snapshots.insert_one({"date": today_str(), "ts": datetime.now(timezone.utc), **pnl})

    async def ltp_loop(self, interval: float) -> None:
        last_snap = 0.0
        while True:
            try:
                if market_open_now() or await order_manager.open_positions():
                    await self.ltp_tick()
                    if asyncio.get_event_loop().time() - last_snap > 60 and market_open_now():
                        await self.snapshot_pnl()
                        last_snap = asyncio.get_event_loop().time()
            except Exception as e:  # noqa: BLE001
                log.warning("ltp loop: %s", e)
            await asyncio.sleep(interval if market_open_now() else 30)

    def start_ltp_loop(self, interval: float) -> None:
        if self._ltp_task is None or self._ltp_task.done():
            self._ltp_task = asyncio.create_task(self.ltp_loop(interval))

    # ------------------------------------------------------------------------------ square-off & EOD
    async def square_off(self, reason: str = "Time square-off") -> int:
        if not self.ltps:
            await self.ltp_tick()
        n = await order_manager.square_off_all(reason, self.ltps)
        await log_event("info", "engine", f"{reason}: closed {n} position(s)")
        return n

    async def end_of_day(self) -> None:
        pnl = await order_manager.day_pnl()
        await self._save_session(eod_pnl=pnl, eod_at=datetime.now(timezone.utc))
        await log_event("info", "engine", f"EOD: {pnl}")


engine = Engine()
