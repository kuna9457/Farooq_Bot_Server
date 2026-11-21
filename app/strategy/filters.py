"""The strategy rules as pure functions over pandas DataFrames.

Used identically by the live engine and the backtester (no look-ahead: callers pass only data known at that time).
All functions are side-aware: BUY rules and their SELL mirrors.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

import numpy as np
import pandas as pd

from ..indicators import chandelier_exit, heikin_ashi, ma_ribbon, volume_profile
from .params import Filter1Params, Filter2Params, Filter3Params, RegimeParams, ScanParams

Side = Literal["BUY", "SELL"]


@dataclass
class FilterResult:
    filter_id: str
    status: Literal["PASS", "FAIL", "PENDING", "ERROR", "DISABLED"]
    reason: str
    values: dict = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.status in ("PASS", "DISABLED")

    def to_dict(self) -> dict:
        return {"filter_id": self.filter_id, "status": self.status, "passed": self.passed,
                "reason": self.reason, "values": _jsonable(self.values)}


def _jsonable(v):
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, (np.floating,)):
        return None if np.isnan(v) else round(float(v), 4)
    if isinstance(v, float):
        return None if np.isnan(v) else round(v, 4)
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.isoformat()
    return v


def r2(x) -> float | None:
    try:
        return None if x is None or np.isnan(x) else round(float(x), 2)
    except TypeError:
        return None


# --------------------------------------------------------------------------------------------- helpers
def resample(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    """Daily -> weekly / monthly OHLCV. Weekly bars are labelled by their first trading day."""
    if tf == "1D":
        return df
    rule = {"1W": "W-FRI", "1M": "ME"}[tf]
    agg = df.resample(rule).agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
    agg = agg.dropna(subset=["open"])
    # relabel to the first actual trading date of each period (nicer on charts)
    first_dates = df.index.to_series().resample(rule).first().dropna()
    agg.index = pd.DatetimeIndex(first_dates.loc[agg.index].array)
    return agg


def with_partial_bar(daily: pd.DataFrame, intraday_today: pd.DataFrame | None) -> pd.DataFrame:
    """Append today's partial daily bar (built from today's intraday candles so far)."""
    if intraday_today is None or intraday_today.empty:
        return daily
    day = intraday_today.index[0].normalize()
    bar = pd.DataFrame({
        "open": [intraday_today["open"].iloc[0]], "high": [intraday_today["high"].max()],
        "low": [intraday_today["low"].min()], "close": [intraday_today["close"].iloc[-1]],
        "volume": [intraday_today["volume"].sum()],
    }, index=pd.DatetimeIndex([day]))
    if daily.index.tz is not None and bar.index.tz is None:
        bar.index = bar.index.tz_localize(daily.index.tz)
    daily = daily[daily.index.normalize() < day]
    return pd.concat([daily, bar])


# --------------------------------------------------------------------------------------------- scanner
def month_extreme_flags(daily: pd.DataFrame, p: ScanParams) -> dict:
    """Uses completed daily bars only (last row = previous session at 08:30)."""
    n = p.month_lookback_days
    if len(daily) < n:
        return {"month_high": False, "month_low": False}
    win = daily.iloc[-n:]
    last = daily.iloc[-1]
    if p.month_basis == "close":
        mh = last["close"] >= win["close"].max()
        ml = last["close"] <= win["close"].min()
    else:
        mh = last["high"] >= win["high"].max()
        ml = last["low"] <= win["low"].min()
    return {"month_high": bool(mh), "month_low": bool(ml),
            "month_high_level": r2(win["high"].max()), "month_low_level": r2(win["low"].min()),
            "last_close": r2(last["close"]), "last_date": daily.index[-1].date().isoformat()}


def pick_candidates(rows: list[dict], top_n: int) -> tuple[list[dict], list[dict]]:
    """rows: [{symbol, month_high, month_low, market_cap, ...}] -> (buy top N, sell top N) by market cap desc."""
    mc = lambda r: r.get("market_cap") or 0  # noqa: E731
    buys = sorted([r for r in rows if r["month_high"]], key=mc, reverse=True)[:top_n]
    sells = sorted([r for r in rows if r["month_low"]], key=mc, reverse=True)[:top_n]
    return buys, sells


# --------------------------------------------------------------------------------------------- regime
def decide_regime(change_points: float, p: RegimeParams) -> Literal["BUY", "SELL", "NO_TRADE"]:
    t = p.threshold_points
    if p.rule == "symmetric":
        if change_points >= t:
            return "BUY"
        if change_points <= -t:
            return "SELL"
        return "NO_TRADE"
    return "BUY" if change_points > t else "SELL"


# --------------------------------------------------------------------------------------------- filter 1
def filter1(daily: pd.DataFrame, side: Side, p: Filter1Params) -> FilterResult:
    """5-year chart: last CE signal (buy for BUY side) -> anchored VP -> latest candle O & C above VAH (below VAL)."""
    if not p.enabled:
        return FilterResult("F1", "DISABLED", "Filter disabled")
    try:
        if daily.empty:
            return FilterResult("F1", "ERROR", "No daily data")
        start = daily.index[-1] - pd.DateOffset(years=p.years)
        bars = resample(daily[daily.index >= start], p.timeframe)
        if len(bars) < p.ce.length + 2:
            return FilterResult("F1", "FAIL", f"Not enough {p.timeframe} bars ({len(bars)})")
        chart = heikin_ashi(bars) if p.use_heikin_ashi else bars
        ce = chandelier_exit(chart, p.ce.length, p.ce.mult, p.ce.use_close)
        sig_col = "buy" if side == "BUY" else "sell"
        sig_idx = np.flatnonzero(ce[sig_col].to_numpy())
        if len(sig_idx) == 0:
            return FilterResult("F1", "FAIL", f"No Chandelier {side} signal in {p.years}Y {p.timeframe} chart",
                                {"ce_dir": int(ce["dir"].iloc[-1])})
        anchor = int(sig_idx[-1])
        anchor_ts = chart.index[anchor]
        vp = volume_profile(chart.iloc[anchor:], p.vp.rows, p.vp.value_area_pct)
        last = chart.iloc[-1]
        if side == "BUY":
            ok = last["open"] > vp.vah and last["close"] > vp.vah
            rel = f"O {last['open']:.2f} / C {last['close']:.2f} {'>' if ok else 'not both >'} VAH {vp.vah:.2f}"
        else:
            ok = last["open"] < vp.val and last["close"] < vp.val
            rel = f"O {last['open']:.2f} / C {last['close']:.2f} {'<' if ok else 'not both <'} VAL {vp.val:.2f}"
        vals = {"anchor_date": anchor_ts.date().isoformat(), "vah": r2(vp.vah), "val": r2(vp.val), "poc": r2(vp.poc),
                "candle_open": r2(last["open"]), "candle_close": r2(last["close"]),
                "ce_dir": int(ce["dir"].iloc[-1]), "timeframe": p.timeframe}
        return FilterResult("F1", "PASS" if ok else "FAIL",
                            f"CE {side.lower()} on {anchor_ts.date()} | {rel}", vals)
    except Exception as e:  # noqa: BLE001
        return FilterResult("F1", "ERROR", f"{type(e).__name__}: {e}")


# --------------------------------------------------------------------------------------------- filter 2
def filter2_setup(daily_completed: pd.DataFrame, side: Side, p: Filter2Params) -> dict | None:
    """Find the most recent setup candle among COMPLETED daily bars.
    BUY setup: CE in BUY (or fresh buy signal) AND selected candle values (default open & close) above upper ribbon."""
    if len(daily_completed) < max(p.ribbon.lengths) + 2:
        return None
    chart = heikin_ashi(daily_completed) if p.use_heikin_ashi else daily_completed
    ce = chandelier_exit(chart, p.ce.length, p.ce.mult, p.ce.use_close)
    rib = ma_ribbon(chart, p.ribbon.lengths, p.ribbon.kind, p.ribbon.source)
    fields = p.candle_fields or ['open', 'close']
    lookback = max(1, p.setup_lookback_days)
    n = len(chart)
    for i in range(n - 1, max(n - 1 - lookback, -1), -1):
        row = chart.iloc[i]
        if side == "BUY":
            ce_ok = bool(ce["buy"].iloc[i]) if p.setup_requires_fresh_signal else ce["dir"].iloc[i] == 1
            band = rib["upper"].iloc[i]
            rib_ok = not np.isnan(band) and all(row[f] > band for f in fields)
        else:
            ce_ok = bool(ce["sell"].iloc[i]) if p.setup_requires_fresh_signal else ce["dir"].iloc[i] == -1
            band = rib["lower"].iloc[i]
            rib_ok = not np.isnan(band) and all(row[f] < band for f in fields)
        if ce_ok and rib_ok:
            # trigger level uses the REAL candle high/low (tradeable price), HA used for the setup decision
            real = daily_completed.iloc[i]
            return {"setup_date": chart.index[i].date().isoformat(),
                    "trigger_level": r2(real["high"] if side == "BUY" else real["low"]),
                    "ribbon_band": r2(band), "ha_open": r2(row["open"]), "ha_close": r2(row["close"]),
                    "ce_dir": int(ce["dir"].iloc[i])}
    return None


def filter2(setup: dict | None, side: Side, price: float | None, p: Filter2Params) -> FilterResult:
    if not p.enabled:
        return FilterResult("F2", "DISABLED", "Filter disabled")
    if setup is None:
        return FilterResult("F2", "FAIL", f"No setup candle (CE {side} + beyond ribbon) in last {p.setup_lookback_days} days")
    if price is None:
        return FilterResult("F2", "PENDING", "Waiting for live price", setup)
    lvl = setup["trigger_level"]
    ok = price > lvl if side == "BUY" else price < lvl
    word = "above" if side == "BUY" else "below"
    vals = dict(setup, price=r2(price))
    return FilterResult("F2", "PASS" if ok else "FAIL",
                        f"Setup {setup['setup_date']} | price {price:.2f} {'is' if ok else 'not'} {word} "
                        f"{'high' if side == 'BUY' else 'low'} {lvl:.2f}", vals)


# --------------------------------------------------------------------------------------------- filter 3
def prev_day_poc(prev_day_5m: pd.DataFrame, p: Filter3Params, ha_context: pd.DataFrame | None = None) -> dict | None:
    """Fixed range VP over the previous trading day's 5m candles (first to last). If use_heikin_ashi, the profile is
    built from HA candles (computed with continuity using ha_context = all 5m bars up to & incl. previous day)."""
    if prev_day_5m is None or prev_day_5m.empty:
        return None
    if p.use_heikin_ashi:
        ctx = ha_context if ha_context is not None and not ha_context.empty else prev_day_5m
        ha = heikin_ashi(ctx)
        bars = ha.loc[prev_day_5m.index.intersection(ha.index)]
    else:
        bars = prev_day_5m
    vp = volume_profile(bars, p.vp.rows, p.vp.value_area_pct)
    if vp is None:
        return None
    return {"poc": r2(vp.poc), "vah": r2(vp.vah), "val": r2(vp.val),
            "date": prev_day_5m.index[0].date().isoformat()}


def filter3(bars_5m: pd.DataFrame, poc_info: dict | None, side: Side, p: Filter3Params) -> FilterResult:
    """bars_5m: recent COMPLETED 5m bars (several days for CE warm-up), last row = latest closed candle."""
    if not p.enabled:
        return FilterResult("F3", "DISABLED", "Filter disabled")
    if poc_info is None:
        return FilterResult("F3", "ERROR", "Previous day POC unavailable")
    if bars_5m is None or len(bars_5m) < p.ce.length + 2:
        return FilterResult("F3", "PENDING", "Waiting for enough 5m candles")
    chart = heikin_ashi(bars_5m) if p.use_heikin_ashi else bars_5m
    ce = chandelier_exit(chart, p.ce.length, p.ce.mult, p.ce.use_close)
    last = chart.iloc[-1]
    d = int(ce["dir"].iloc[-1])
    poc = poc_info["poc"]
    if side == "BUY":
        ce_ok = d == 1
        pos_ok = last["close"] > poc and (p.above_poc_rule == "close" or last["open"] > poc)
        word = "above"
    else:
        ce_ok = d == -1
        pos_ok = last["close"] < poc and (p.above_poc_rule == "close" or last["open"] < poc)
        word = "below"
    vals = {"poc": poc, "ce_dir": d, "ce_stop": r2(ce["long_stop"].iloc[-1] if side == "BUY" else ce["short_stop"].iloc[-1]),
            "candle_open": r2(last["open"]), "candle_close": r2(last["close"]),
            "candle_time": chart.index[-1].isoformat()}
    reason = (f"CE 5m {'BUY' if d == 1 else 'SELL'} {'✔' if ce_ok else '✘'} | "
              f"candle {last['close']:.2f} {word} POC {poc:.2f} {'✔' if pos_ok else '✘'}")
    return FilterResult("F3", "PASS" if (ce_ok and pos_ok) else "FAIL", reason, vals)


def exit_signal_5m(bars_5m: pd.DataFrame, side: Side, p: Filter3Params) -> tuple[bool, dict]:
    """Opposite Chandelier signal (state flip) on 5m -> exit."""
    if bars_5m is None or len(bars_5m) < p.ce.length + 2:
        return False, {}
    chart = heikin_ashi(bars_5m) if p.use_heikin_ashi else bars_5m
    ce = chandelier_exit(chart, p.ce.length, p.ce.mult, p.ce.use_close)
    d = int(ce["dir"].iloc[-1])
    stop = ce["long_stop"].iloc[-1] if side == "BUY" else ce["short_stop"].iloc[-1]
    flip = (side == "BUY" and d == -1) or (side == "SELL" and d == 1)
    return flip, {"ce_dir": d, "ce_stop": r2(stop)}
