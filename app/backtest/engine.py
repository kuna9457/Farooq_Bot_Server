"""Bar-by-bar backtester. Re-uses the exact filter functions of the live engine.

No look-ahead rules:
  * 08:30 scan uses daily bars strictly before day D.
  * Regime uses the Nifty 5m bar that CLOSES at the decision time (default 09:25 bar -> 09:30).
  * Filters are evaluated at each 5m bar close; entries/exits fill at the NEXT bar's REAL open (+slippage).
  * Stop-loss / target are checked intrabar on real high/low.
"""
from __future__ import annotations

import asyncio
import logging
import math
import traceback
from datetime import date, datetime, time, timedelta, timezone

import numpy as np
import pandas as pd
from bson import ObjectId

from ..core.events import hub
from ..data.market_data import market_data
from ..data.universe import NIFTY_INDEX, get_universe, refresh_fundamentals
from ..db import db
from ..execution.charges import intraday_charges
from ..strategy.evaluator import prepare_day
from ..strategy.filters import (_jsonable, decide_regime, exit_signal_5m, filter1, filter2, filter3, month_extreme_flags,
                                pick_candidates, with_partial_bar)
from ..strategy.params import StrategyParams

log = logging.getLogger("backtest")
IST = "Asia/Kolkata"


def _t(s: str) -> time:
    return datetime.strptime(s, "%H:%M").time()


class Backtester:
    def __init__(self, run_id: str, p: StrategyParams, start: date, end: date, capital: float):
        self.run_id, self.p, self.start, self.end, self.capital = run_id, p, start, end, capital
        self.trades: list[dict] = []
        self.days: list[dict] = []
        self.filter_stats = {"F1": [0, 0], "F2": [0, 0], "F3": [0, 0]}  # [pass, evaluated]

    # ------------------------------------------------------------------ data loading (async)
    async def load(self, progress) -> None:
        universe = await get_universe()
        self.symbols = [u["symbol"] for u in universe]
        self.shares = await refresh_fundamentals()
        self.daily: dict[str, pd.DataFrame] = {}
        sem = asyncio.Semaphore(6)

        async def ld(s):
            async with sem:
                try:
                    self.daily[s] = await market_data.daily(s)
                except Exception as e:  # noqa: BLE001
                    log.warning("daily %s: %s", s, e)
        await asyncio.gather(*[ld(s) for s in self.symbols + [NIFTY_INDEX]])
        await progress(10, "Daily data loaded")
        warm = self.start - timedelta(days=12)
        self.nifty5 = await market_data.history_5m(NIFTY_INDEX, warm, self.end)
        if self.nifty5.empty:
            raise RuntimeError("No Nifty 5m data for the range (yfinance only has ~60 days of 5m history)")
        self.bars5: dict[str, pd.DataFrame] = {}
        # 5m history is loaded lazily only for stocks that become candidates (see _bars5)
        self._warm = warm

    async def preload_5m(self, symbols: set[str], progress) -> None:
        sem = asyncio.Semaphore(4)
        done = 0

        async def ld(s):
            nonlocal done
            async with sem:
                self.bars5[s] = await market_data.history_5m(s, self._warm, self.end)
                done += 1
                await progress(10 + int(30 * done / max(1, len(symbols))), f"5m data {done}/{len(symbols)}")
        await asyncio.gather(*[ld(s) for s in symbols if s not in self.bars5])

    def plan_days(self) -> list[tuple[date, list[dict], list[dict], dict]]:
        """Run the 08:30 scan for each trading day (pure, cheap)."""
        days = sorted({d for d in self.nifty5.index.date if self.start <= d <= self.end})
        out = []
        for D in days:
            cut = pd.Timestamp(D, tz=IST)
            rows = []
            for s in self.symbols:
                df = self.daily.get(s)
                if df is None or df.empty:
                    continue
                dc = df[df.index < cut]
                f = month_extreme_flags(dc, self.p.scan)
                if not f.get("last_close"):
                    continue
                sh = self.shares.get(s)
                rows.append({"symbol": s, "market_cap": (sh or 0) * f["last_close"], **f})
            b, sl = pick_candidates(rows, self.p.scan.top_n)
            mc = lambda r: r.get("market_cap") or 0  # noqa: E731
            hi = sorted([r for r in rows if r["month_high"]], key=mc, reverse=True)
            lo = sorted([r for r in rows if r["month_low"]], key=mc, reverse=True)
            info = {"scanned": len(rows), "high_count": len(hi), "low_count": len(lo),
                    "high_symbols": [r["symbol"] for r in hi][:50], "low_symbols": [r["symbol"] for r in lo][:50]}
            out.append((D, b, sl, info))
        return out

    # ------------------------------------------------------------------ simulation (sync, run in thread)
    def simulate(self, plan) -> None:
        p, r = self.p, self.p.risk
        dec_t = _t(p.regime.decision_time)
        sq_t, last_entry_t = _t(r.square_off_time), _t(r.last_entry_time)
        slip = r.slippage_pct / 100
        nd = self.daily[NIFTY_INDEX]
        for D, buys, sells, info in plan:
            cut = pd.Timestamp(D, tz=IST)
            nprev = nd[nd.index < cut]
            nday = self.nifty5[self.nifty5.index.date == D]
            dec_bar = nday[nday.index + pd.Timedelta(minutes=5) <= pd.Timestamp.combine(D, dec_t).tz_localize(IST)]
            if nprev.empty or dec_bar.empty:
                continue
            change = float(dec_bar["close"].iloc[-1] - nprev["close"].iloc[-1])
            regime = decide_regime(change, p.regime)
            if regime in ("BUY", "SELL") and regime not in p.sides_enabled:
                regime = "NO_TRADE"
            day = {"date": D.isoformat(), "nifty_change": round(change, 2), "regime": regime,
                   "buy_candidates": [x["symbol"] for x in buys], "sell_candidates": [x["symbol"] for x in sells],
                   "trades": 0, "net_pnl": 0.0, "scan": info, "stocks": []}
            self.days.append(day)
            # one record per scanned candidate (both sides) - what the scan found and how each filter did
            recs: dict[str, dict] = {}
            for sd, lst in (("BUY", buys), ("SELL", sells)):
                for rank, c in enumerate(lst, 1):
                    recs[(sd, c["symbol"])] = {
                        "symbol": c["symbol"], "side": sd, "rank": rank,
                        "market_cap_cr": round((c.get("market_cap") or 0) / 1e7),
                        "last_close": c.get("last_close"),
                        "month_level": c.get("month_high_level") if sd == "BUY" else c.get("month_low_level"),
                        "active": sd == regime, "note": "" if sd == regime else
                        ("Not traded: regime is NO TRADE" if regime == "NO_TRADE" else f"Not traded: {regime} day"),
                        "F1": None, "F2": None, "F3": None, "signal_time": None, "trades": []}
            day["stocks"] = list(recs.values())
            if regime == "NO_TRADE":
                continue
            cands = buys if regime == "BUY" else sells
            side = regime
            ctx = {}
            for c in cands:
                s = c["symbol"]
                rec = recs[(side, s)]
                b5 = self.bars5.get(s)
                if b5 is None or b5.empty:
                    rec["note"] = "No 5-minute data for this stock"
                    continue
                before = b5[b5.index < cut].iloc[-900:]
                today = b5[b5.index.date == D]
                if today.empty:
                    rec["note"] = "No 5-minute data for this day"
                    continue
                dc = self.daily[s][self.daily[s].index < cut]
                prep = prepare_day(dc, before, side, p)
                rec["setup"] = _jsonable(prep["setup"]) if prep.get("setup") else None
                rec["poc"] = _jsonable(prep["poc"]) if prep.get("poc") else None
                ctx[s] = {"before": before, "today": today, "daily": dc, "prep": prep,
                          "pos": None, "traded": False, "f1_logged": False, "rec": rec}
            # time grid of the day
            grid = sorted(set().union(*[set(v["today"].index) for v in ctx.values()])) if ctx else []
            day_realized = 0.0
            halted = False
            pending: list[tuple] = []   # (symbol, action) executed at next bar open
            for ts in grid:
                bar_close_t = (ts + pd.Timedelta(minutes=5)).time()
                # 1) execute pending orders at this bar's open
                for s, action in pending:
                    v = ctx[s]
                    if ts not in v["today"].index:
                        continue
                    o = float(v["today"].loc[ts, "open"])
                    if action == "ENTRY" and v["pos"] is None:
                        price = o * (1 + slip if side == "BUY" else 1 - slip)
                        qty = int(r.capital_per_trade // price)
                        if qty >= 1:
                            sgn = 1 if side == "BUY" else -1
                            sl = price * (1 - sgn * r.stop_loss_pct / 100) if r.stop_loss_type == "percent" and r.stop_loss_pct > 0 else None
                            if r.stop_loss_type == "ce_stop":
                                sl = v.get("ce_stop")
                            tgt = price * (1 + sgn * r.target_pct / 100) if r.target_pct > 0 else None
                            v["pos"] = {"symbol": s, "side": side, "qty": qty, "entry_price": price, "entry_time": ts,
                                        "sl": sl, "target": tgt}
                            v["traded"] = True
                    elif action.startswith("EXIT") and v["pos"] is not None:
                        price = o * (1 - slip if side == "BUY" else 1 + slip)
                        day_realized += self._close(v, price, ts, action[5:], day)
                pending = []
                open_count = sum(1 for v in ctx.values() if v["pos"])
                for s, v in ctx.items():
                    if ts not in v["today"].index:
                        continue
                    bar = v["today"].loc[ts]
                    pos = v["pos"]
                    # 2) intrabar SL / target on real prices
                    if pos:
                        long = side == "BUY"
                        if pos["sl"] and ((long and bar["low"] <= pos["sl"]) or (not long and bar["high"] >= pos["sl"])):
                            px = min(bar["open"], pos["sl"]) if long else max(bar["open"], pos["sl"])
                            day_realized += self._close(v, float(px), ts, f"Stop-loss {pos['sl']:.2f}", day)
                            continue
                        if pos["target"] and ((long and bar["high"] >= pos["target"]) or (not long and bar["low"] <= pos["target"])):
                            px = max(bar["open"], pos["target"]) if long else min(bar["open"], pos["target"])
                            day_realized += self._close(v, float(px), ts, f"Target {pos['target']:.2f}", day)
                            continue
                    # 3) time square-off: exit at open of the square-off bar
                    if ts.time() >= sq_t:
                        if pos:
                            px = float(bar["open"]) * (1 - slip if side == "BUY" else 1 + slip)
                            day_realized += self._close(v, px, ts, "Time square-off", day)
                        continue
                    if bar_close_t < dec_t:
                        continue
                    hist = pd.concat([v["before"], v["today"][v["today"].index <= ts]]).iloc[-300:]
                    # 4) exit signal on bar close
                    if pos:
                        flip, _ = exit_signal_5m(hist, side, p.filter3)
                        if flip and ts > pos["entry_time"]:
                            pending.append((s, "EXIT:Chandelier 5m opposite signal"))
                        continue
                    if v["traded"] and r.one_trade_per_stock_per_day:
                        continue
                    if halted or bar_close_t > last_entry_t or open_count >= r.max_open_positions:
                        continue
                    # 5) entry evaluation (cheap filters first)
                    f3 = filter3(hist, v["prep"]["poc"], side, p.filter3)
                    f2 = filter2(v["prep"]["setup"], side, float(bar["close"]), p.filter2)
                    self._stat("F3", f3.passed)
                    self._stat("F2", f2.passed)
                    self._note(v["rec"], "F3", f3, bar_close_t)
                    self._note(v["rec"], "F2", f2, bar_close_t)
                    if not (f3.passed and f2.passed) and v["f1_logged"]:
                        continue
                    daily_live = with_partial_bar(v["daily"], v["today"][v["today"].index <= ts])
                    f1 = filter1(daily_live, side, p.filter1)
                    self._note(v["rec"], "F1", f1, bar_close_t)
                    if not v["f1_logged"]:
                        self._stat("F1", f1.passed)
                        v["f1_logged"] = True
                    if f1.passed and f2.passed and f3.passed:
                        v["ce_stop"] = f3.values.get("ce_stop")
                        v["rec"]["signal_time"] = bar_close_t.strftime("%H:%M")
                        pending.append((s, "ENTRY"))
                        open_count += 1
                if r.daily_loss_limit > 0 and day_realized <= -r.daily_loss_limit:
                    halted = True
            # safety: close anything still open at the last bar close
            for s, v in ctx.items():
                if v["pos"]:
                    last = v["today"].iloc[-1]
                    day_realized += self._close(v, float(last["close"]), v["today"].index[-1], "End of data", day)
            day["net_pnl"] = round(day_realized, 2)

    @staticmethod
    def _note(rec: dict, key: str, f, t) -> None:
        """Accumulate one filter's outcome over the day's evaluations (bars passed / evaluated, latest reason)."""
        cur = rec.get(key) or {"evaluations": 0, "passes": 0, "first_pass": None}
        cur["evaluations"] += 1
        cur["passes"] += int(f.passed)
        if f.passed and cur["first_pass"] is None:
            cur["first_pass"] = t.strftime("%H:%M")
        cur["status"], cur["reason"] = f.status, f.reason
        cur["values"] = {k: x for k, x in _jsonable(f.values).items()
                         if x is None or isinstance(x, (int, float, str, bool))}
        rec[key] = cur

    def _stat(self, k, ok):
        self.filter_stats[k][1] += 1
        self.filter_stats[k][0] += int(ok)

    def _close(self, v, price, ts, reason, day) -> float:
        pos = v["pos"]
        sgn = 1 if pos["side"] == "BUY" else -1
        gross = sgn * (price - pos["entry_price"]) * pos["qty"]
        bv = (pos["entry_price"] if sgn == 1 else price) * pos["qty"]
        sv = (price if sgn == 1 else pos["entry_price"]) * pos["qty"]
        ch = intraday_charges(bv, sv)
        net = gross - ch
        self.trades.append({"run_id": self.run_id, "date": ts.date().isoformat(), "symbol": pos["symbol"],
                            "side": pos["side"], "qty": pos["qty"], "entry_price": round(pos["entry_price"], 2),
                            "entry_time": pos["entry_time"].isoformat(), "exit_price": round(price, 2),
                            "exit_time": ts.isoformat(), "gross_pnl": round(gross, 2), "charges": ch,
                            "net_pnl": round(net, 2), "exit_reason": reason,
                            "return_pct": round(sgn * (price / pos["entry_price"] - 1) * 100, 3)})
        day["trades"] += 1
        if v.get("rec") is not None:
            v["rec"]["trades"].append({"entry_time": pos["entry_time"].strftime("%H:%M"), "exit_time": ts.strftime("%H:%M"),
                                       "entry_price": round(pos["entry_price"], 2), "exit_price": round(price, 2),
                                       "net_pnl": round(net, 2), "exit_reason": reason})
        v["pos"] = None
        return net

    # ------------------------------------------------------------------ metrics
    def metrics(self) -> dict:
        t = pd.DataFrame(self.trades)
        days = pd.DataFrame(self.days)
        base = {"trading_days": len(days),
                "buy_days": int((days["regime"] == "BUY").sum()) if len(days) else 0,
                "sell_days": int((days["regime"] == "SELL").sum()) if len(days) else 0,
                "no_trade_days": int((days["regime"] == "NO_TRADE").sum()) if len(days) else 0,
                "filter_pass_rate": {k: (round(100 * a / b, 1) if b else None) for k, (a, b) in self.filter_stats.items()},
                "filter_evaluations": {k: b for k, (a, b) in self.filter_stats.items()}}
        if t.empty:
            return {**base, "total_trades": 0, "net_pnl": 0, "equity_curve": self._equity(days)}
        wins = t[t["net_pnl"] > 0]
        losses = t[t["net_pnl"] <= 0]
        eq = self._equity(days)
        peak, mdd = self.capital, 0.0
        for e in eq:
            peak = max(peak, e["equity"])
            mdd = min(mdd, e["equity"] - peak)
        daily_ret = days["net_pnl"] / self.capital if len(days) else pd.Series(dtype=float)
        sharpe = float(daily_ret.mean() / daily_ret.std() * math.sqrt(252)) if len(daily_ret) > 1 and daily_ret.std() > 0 else None
        gp, gl = wins["net_pnl"].sum(), -losses["net_pnl"].sum()
        return {**base,
                "total_trades": int(len(t)), "wins": int(len(wins)), "losses": int(len(losses)),
                "win_rate": round(100 * len(wins) / len(t), 1),
                "gross_pnl": round(float(t["gross_pnl"].sum()), 2), "charges": round(float(t["charges"].sum()), 2),
                "net_pnl": round(float(t["net_pnl"].sum()), 2),
                "avg_win": round(float(wins["net_pnl"].mean()), 2) if len(wins) else 0,
                "avg_loss": round(float(losses["net_pnl"].mean()), 2) if len(losses) else 0,
                "profit_factor": round(float(gp / gl), 2) if gl > 0 else None,
                "expectancy": round(float(t["net_pnl"].mean()), 2),
                "max_drawdown": round(mdd, 2), "max_drawdown_pct": round(100 * mdd / self.capital, 2),
                "return_pct": round(100 * float(t["net_pnl"].sum()) / self.capital, 2),
                "sharpe": round(sharpe, 2) if sharpe is not None else None,
                "days_with_trades": int((days["trades"] > 0).sum()),
                "by_exit_reason": t.groupby(t["exit_reason"].str.split(" ").str[0])["net_pnl"].agg(["count", "sum"]).round(2).reset_index().to_dict("records"),
                "by_symbol": t.groupby("symbol")["net_pnl"].agg(["count", "sum"]).round(2).reset_index().to_dict("records"),
                "equity_curve": eq}

    def _equity(self, days: pd.DataFrame) -> list[dict]:
        eq, out = self.capital, []
        for _, d in days.iterrows():
            eq += d["net_pnl"]
            out.append({"date": d["date"], "equity": round(eq, 2), "pnl": d["net_pnl"], "regime": d["regime"]})
        return out


# ====================================================================== runner
async def run_backtest(run_id: str, p: StrategyParams, start: date, end: date, capital: float) -> None:
    coll = db().backtest_runs

    async def progress(pct, msg):
        await coll.update_one({"_id": ObjectId(run_id)}, {"$set": {"progress": pct, "message": msg}})
        await hub.broadcast("backtest_progress", {"run_id": run_id, "progress": pct, "message": msg})

    try:
        await coll.update_one({"_id": ObjectId(run_id)}, {"$set": {"status": "RUNNING"}})
        bt = Backtester(run_id, p, start, end, capital)
        await progress(2, "Loading data")
        await bt.load(progress)
        plan = await asyncio.to_thread(bt.plan_days)
        need = {c["symbol"] for _, b, s, _i in plan for c in b + s}
        await progress(12, f"{len(plan)} trading days, {len(need)} candidate stocks")
        await bt.preload_5m(need, progress)
        await progress(45, "Simulating")
        await asyncio.to_thread(bt.simulate, plan)
        m = await asyncio.to_thread(bt.metrics)
        if bt.trades:
            await db().backtest_trades.insert_many([dict(x) for x in bt.trades])
        await coll.update_one({"_id": ObjectId(run_id)},
                              {"$set": {"status": "DONE", "progress": 100, "message": "Done", "metrics": m,
                                        "days": bt.days, "finished_at": datetime.now(timezone.utc)}})
        await hub.broadcast("backtest_progress", {"run_id": run_id, "progress": 100, "message": "Done", "status": "DONE"})
    except Exception as e:  # noqa: BLE001
        log.exception("backtest failed")
        await coll.update_one({"_id": ObjectId(run_id)},
                              {"$set": {"status": "FAILED", "message": f"{type(e).__name__}: {e}",
                                        "traceback": traceback.format_exc()[-3000:]}})
        await hub.broadcast("backtest_progress", {"run_id": run_id, "status": "FAILED", "message": str(e)})
