"""Order manager: entries/exits, positions, trades, P&L, risk checks. Works for paper and live modes."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from ..core.events import hub, log_event
from ..core.state import get_params, get_runtime
from ..db import clean, db
from .brokers import BrokerError, PaperBroker, get_broker
from .charges import intraday_charges

log = logging.getLogger("orders")


def today_str() -> str:
    from ..data.market_data import now_ist
    return now_ist().date().isoformat()


class OrderManager:
    def __init__(self):
        self._lock = asyncio.Lock()

    async def _broker(self):
        rt = await get_runtime()
        if rt["mode"] == "live":
            return get_broker(rt["broker"]), "live", rt["broker"]
        return PaperBroker(), "paper", "paper"

    async def open_positions(self, mode: str | None = None) -> list[dict]:
        q = {"status": "OPEN"}
        if mode:
            q["mode"] = mode
        return [p async for p in db().positions.find(q)]

    # --------------------------------------------------------------------------------- risk
    async def can_enter(self, symbol: str) -> tuple[bool, str]:
        rt = await get_runtime()
        p = (await get_params()).risk
        today = today_str()
        if not rt["engine_enabled"]:
            return False, "Auto-trading disabled"
        if rt.get("halted_date") == today:
            return False, "Trading halted for today (kill switch / loss limit)"
        mode = rt["mode"]
        open_pos = await self.open_positions(mode)
        if any(x["symbol"] == symbol for x in open_pos):
            return False, "Position already open"
        if len(open_pos) >= p.max_open_positions:
            return False, f"Max open positions ({p.max_open_positions}) reached"
        if p.one_trade_per_stock_per_day and await db().trades.find_one({"date": today, "symbol": symbol, "mode": mode}):
            return False, "Already traded this stock today"
        return True, "ok"

    # --------------------------------------------------------------------------------- entry
    async def enter(self, symbol: str, side: str, ltp: float, context: dict | None = None) -> dict | None:
        async with self._lock:
            ok, why = await self.can_enter(symbol)
            if not ok:
                await log_event("info", "orders", f"{symbol} entry skipped: {why}")
                return None
            params = await get_params()
            r = params.risk
            qty = int(r.capital_per_trade // ltp)
            if qty < 1:
                await log_event("warning", "orders", f"{symbol}: capital_per_trade too small for price {ltp}")
                return None
            broker, mode, bname = await self._broker()
            fill_hint = ltp * (1 + r.slippage_pct / 100 * (1 if side == "BUY" else -1)) if mode == "paper" else ltp
            try:
                res = await broker.place_order(symbol, side, qty, ltp=fill_hint, tag=f"ENTRY-{symbol}")
            except BrokerError as e:
                await log_event("error", "orders", f"{symbol} ENTRY order failed: {e}")
                return None
            price = float(res["avg_price"])
            sl, target = self._levels(side, price, r, context)
            now = datetime.now(timezone.utc)
            await db().orders.insert_one({"date": today_str(), "ts": now, "symbol": symbol, "side": side, "qty": qty,
                                          "purpose": "ENTRY", "order_id": res["order_id"], "status": res["status"],
                                          "avg_price": price, "mode": mode, "broker": bname})
            pos = {"date": today_str(), "symbol": symbol, "side": side, "qty": qty, "entry_price": price,
                   "entry_time": now, "sl": sl, "target": target, "mode": mode, "broker": bname,
                   "status": "OPEN", "entry_order_id": res["order_id"], "ltp": price, "unrealized": 0.0,
                   "context": context or {}}
            ins = await db().positions.insert_one(pos)
            pos["_id"] = ins.inserted_id
            await db().signals.insert_one({"date": today_str(), "ts": now, "symbol": symbol, "side": side,
                                           "type": "ENTRY", "price": price, "reason": "F1+F2+F3 passed"})
            await log_event("info", "orders", f"ENTRY {side} {qty} {symbol} @ {price:.2f} ({mode}) SL {sl} TGT {target}")
            await hub.broadcast("position_update", clean(pos))
            return pos

    @staticmethod
    def _levels(side, price, r, ctx):
        sl = target = None
        sgn = 1 if side == "BUY" else -1
        if r.stop_loss_type == "percent" and r.stop_loss_pct > 0:
            sl = round(price * (1 - sgn * r.stop_loss_pct / 100), 2)
        elif r.stop_loss_type == "ce_stop" and ctx:
            sl = (ctx.get("F3") or {}).get("values", {}).get("ce_stop")
        if r.target_pct > 0:
            target = round(price * (1 + sgn * r.target_pct / 100), 2)
        return sl, target

    # --------------------------------------------------------------------------------- exit
    async def exit(self, pos: dict, ltp: float, reason: str) -> dict | None:
        async with self._lock:
            pos = await db().positions.find_one({"_id": pos["_id"], "status": "OPEN"})
            if not pos:
                return None
            r = (await get_params()).risk
            if pos["mode"] == "live":
                broker = get_broker(pos["broker"])
            else:
                broker = PaperBroker()
            exit_side = "SELL" if pos["side"] == "BUY" else "BUY"
            hint = ltp * (1 + r.slippage_pct / 100 * (1 if exit_side == "BUY" else -1)) if pos["mode"] == "paper" else ltp
            try:
                res = await broker.place_order(pos["symbol"], exit_side, pos["qty"], ltp=hint, tag=f"EXIT-{pos['symbol']}")
            except BrokerError as e:
                await log_event("error", "orders", f"{pos['symbol']} EXIT order failed: {e} — MANUAL ACTION NEEDED")
                return None
            price = float(res["avg_price"])
            now = datetime.now(timezone.utc)
            qty = pos["qty"]
            sgn = 1 if pos["side"] == "BUY" else -1
            gross = round(sgn * (price - pos["entry_price"]) * qty, 2)
            buy_val = (pos["entry_price"] if pos["side"] == "BUY" else price) * qty
            sell_val = (price if pos["side"] == "BUY" else pos["entry_price"]) * qty
            charges = intraday_charges(buy_val, sell_val)
            trade = {"date": pos["date"], "symbol": pos["symbol"], "side": pos["side"], "qty": qty,
                     "entry_price": pos["entry_price"], "entry_time": pos["entry_time"], "exit_price": price,
                     "exit_time": now, "gross_pnl": gross, "charges": charges, "net_pnl": round(gross - charges, 2),
                     "exit_reason": reason, "mode": pos["mode"], "broker": pos["broker"], "position_id": str(pos["_id"])}
            await db().trades.insert_one(dict(trade))
            await db().orders.insert_one({"date": pos["date"], "ts": now, "symbol": pos["symbol"], "side": exit_side,
                                          "qty": qty, "purpose": "EXIT", "order_id": res["order_id"],
                                          "status": res["status"], "avg_price": price, "mode": pos["mode"],
                                          "broker": pos["broker"]})
            await db().positions.update_one({"_id": pos["_id"]}, {"$set": {"status": "CLOSED", "exit_price": price,
                                                                          "exit_time": now, "exit_reason": reason}})
            await db().signals.insert_one({"date": pos["date"], "ts": now, "symbol": pos["symbol"], "side": exit_side,
                                           "type": "EXIT", "price": price, "reason": reason})
            await log_event("info", "orders", f"EXIT {pos['symbol']} @ {price:.2f} ({reason}) net P&L ₹{trade['net_pnl']:.2f}")
            await hub.broadcast("trade", clean(trade))
            await hub.broadcast("position_update", {**clean(pos), "status": "CLOSED"})
            return trade

    async def square_off_all(self, reason: str, ltps: dict[str, float]) -> int:
        n = 0
        for p in await self.open_positions():
            price = ltps.get(p["symbol"]) or p.get("ltp") or p["entry_price"]
            if await self.exit(p, price, reason):
                n += 1
        return n

    # --------------------------------------------------------------------------------- mtm
    async def mark_to_market(self, ltps: dict[str, float]) -> list[dict]:
        out = []
        for p in await self.open_positions():
            ltp = ltps.get(p["symbol"])
            if ltp is None:
                out.append(p)
                continue
            sgn = 1 if p["side"] == "BUY" else -1
            unreal = round(sgn * (ltp - p["entry_price"]) * p["qty"], 2)
            await db().positions.update_one({"_id": p["_id"]}, {"$set": {"ltp": ltp, "unrealized": unreal}})
            p.update(ltp=ltp, unrealized=unreal)
            out.append(p)
        return out

    async def day_pnl(self, mode: str | None = None) -> dict:
        rt = await get_runtime()
        mode = mode or rt["mode"]
        realized = 0.0
        n = wins = 0
        async for t in db().trades.find({"date": today_str(), "mode": mode}):
            realized += t["net_pnl"]
            n += 1
            wins += t["net_pnl"] > 0
        unreal = sum(p.get("unrealized", 0) for p in await self.open_positions(mode))
        return {"realized": round(realized, 2), "unrealized": round(unreal, 2), "total": round(realized + unreal, 2),
                "trades": n, "wins": wins, "mode": mode}


order_manager = OrderManager()
